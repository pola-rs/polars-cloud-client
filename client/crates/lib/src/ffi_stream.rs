//! Exports query results over the Arrow C stream interface.
//!
//! Arrow has no 128-bit integer types, so the compute plane sends them as 16-byte little-endian
//! binary. An extension type on the field names the original type. arrow-rs can't describe these
//! integers in a C schema either, so this module builds the schema itself and gives them Polars'
//! own format strings, `_pli128` and `_plu128`. The array data needs no change: a 16-byte binary
//! array has the same buffers as a 128-bit integer array.

use std::ffi::{CString, c_char, c_int, c_void};

use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{Array, RecordBatchReader, StructArray};
use arrow_schema::ffi::Flags;
use arrow_schema::{ArrowError, DataType, Field, Schema};

// These must match the names the compute plane sends.
const INT128_EXTENSION: &str = "polars_nanook.int128";
const UINT128_EXTENSION: &str = "polars_nanook.uint128";

/// The C error code for a failed stream call.
const EIO: c_int = 5;

/// Returns the Polars format string for a field holding 128-bit integers, if it is one.
fn int128_format(field: &Field) -> Option<&'static str> {
    match (field.data_type(), field.extension_type_name()) {
        (DataType::FixedSizeBinary(16), Some(INT128_EXTENSION)) => Some("_pli128"),
        (DataType::FixedSizeBinary(16), Some(UINT128_EXTENSION)) => Some("_plu128"),
        _ => None,
    }
}

fn child_fields(dtype: &DataType) -> Vec<&Field> {
    match dtype {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => vec![field.as_ref()],
        DataType::Struct(fields) => fields.iter().map(|field| field.as_ref()).collect(),
        _ => vec![],
    }
}

fn contains_int128(field: &Field) -> bool {
    int128_format(field).is_some()
        || child_fields(field.data_type())
            .into_iter()
            .any(contains_int128)
}

/// Builds the C schema of `field`, like arrow-rs does, but with Polars' format strings for
/// 128-bit integers.
fn export_field(field: &Field) -> Result<FFI_ArrowSchema, ArrowError> {
    if !contains_int128(field) {
        return FFI_ArrowSchema::try_from(field);
    }
    let (format, children, metadata) = if let Some(format) = int128_format(field) {
        // Polars would load the column as an extension type if the name stayed.
        let metadata: Vec<_> = field
            .metadata()
            .iter()
            .filter(|(key, _)| !key.starts_with("ARROW:extension:"))
            .collect();
        (format.to_owned(), vec![], metadata)
    } else {
        // Only the format of this node is used. Its children are rebuilt below.
        let format = FFI_ArrowSchema::try_from(field.data_type())?
            .format()
            .to_owned();
        let children = child_fields(field.data_type())
            .into_iter()
            .map(export_field)
            .collect::<Result<_, _>>()?;
        (format, children, field.metadata().iter().collect())
    };

    let mut flags = Flags::empty();
    flags.set(Flags::NULLABLE, field.is_nullable());
    flags.set(
        Flags::MAP_KEYS_SORTED,
        matches!(field.data_type(), DataType::Map(_, true)),
    );
    let schema = FFI_ArrowSchema::try_new(&format, children, None)?
        .with_name(field.name())?
        .with_flags(flags)?;
    // SAFETY: `try_new` built this schema.
    unsafe { schema.with_metadata(metadata) }
}

fn export_schema(schema: &Schema) -> Result<FFI_ArrowSchema, ArrowError> {
    let children = schema
        .fields()
        .iter()
        .map(|field| export_field(field))
        .collect::<Result<_, _>>()?;
    let c_schema = FFI_ArrowSchema::try_new("+s", children, None)?;
    // SAFETY: `try_new` built this schema.
    unsafe { c_schema.with_metadata(schema.metadata()) }
}

/// The `ArrowArrayStream` struct of the C stream interface.
///
/// arrow-rs has `FFI_ArrowArrayStream`, but it always derives the schema from arrow-rs types,
/// which can't express 128-bit integers.
#[repr(C)]
pub(crate) struct ArrowArrayStream {
    get_schema: Option<unsafe extern "C" fn(*mut Self, *mut FFI_ArrowSchema) -> c_int>,
    get_next: Option<unsafe extern "C" fn(*mut Self, *mut FFI_ArrowArray) -> c_int>,
    get_last_error: Option<unsafe extern "C" fn(*mut Self) -> *const c_char>,
    release: Option<unsafe extern "C" fn(*mut Self)>,
    private_data: *mut c_void,
}

// SAFETY: `private_data` owns a `StreamState`, which is `Send`.
unsafe impl Send for ArrowArrayStream {}

struct StreamState {
    reader: Box<dyn RecordBatchReader + Send>,
    last_error: Option<CString>,
}

impl StreamState {
    /// Keeps `err` for `get_last_error` and returns the error code.
    fn fail(&mut self, err: ArrowError) -> c_int {
        self.last_error = Some(CString::new(err.to_string()).unwrap_or_default());
        EIO
    }
}

impl ArrowArrayStream {
    pub(crate) fn new(reader: Box<dyn RecordBatchReader + Send>) -> Self {
        let state = Box::new(StreamState {
            reader,
            last_error: None,
        });
        Self {
            get_schema: Some(get_schema),
            get_next: Some(get_next),
            get_last_error: Some(get_last_error),
            release: Some(release),
            private_data: Box::into_raw(state).cast(),
        }
    }
}

impl Drop for ArrowArrayStream {
    fn drop(&mut self) {
        // A consumer that moved the stream out has cleared `release`.
        if let Some(release) = self.release {
            // SAFETY: `release` is still set, so the stream isn't released yet.
            unsafe { release(self) }
        }
    }
}

/// # Safety
/// `stream` must point to a stream from [`ArrowArrayStream::new`] that isn't released yet.
unsafe fn state<'a>(stream: *mut ArrowArrayStream) -> &'a mut StreamState {
    // SAFETY: the stream is live, so `private_data` is the `StreamState` that `new` boxed.
    unsafe { &mut *(*stream).private_data.cast::<StreamState>() }
}

// The callbacks below rely on the C stream interface contract. The consumer calls them only on a
// stream that isn't released, and passes `out` pointers to structs it owns and hasn't initialized.

unsafe extern "C" fn get_schema(stream: *mut ArrowArrayStream, out: *mut FFI_ArrowSchema) -> c_int {
    // SAFETY: the stream isn't released, by the C stream interface contract.
    let state = unsafe { state(stream) };
    match export_schema(&state.reader.schema()) {
        Ok(schema) => {
            // SAFETY: `out` is valid for writes. `write` doesn't drop its uninitialized contents.
            unsafe { out.write(schema) };
            0
        },
        Err(err) => state.fail(err),
    }
}

unsafe extern "C" fn get_next(stream: *mut ArrowArrayStream, out: *mut FFI_ArrowArray) -> c_int {
    // SAFETY: the stream isn't released, by the C stream interface contract.
    let state = unsafe { state(stream) };
    match state.reader.next() {
        // A released array marks the end of the stream.
        None => {
            // SAFETY: `out` is valid for writes. `write` doesn't drop its uninitialized contents.
            unsafe { out.write(FFI_ArrowArray::empty()) };
            0
        },
        Some(Ok(batch)) => {
            let array = FFI_ArrowArray::new(&StructArray::from(batch).to_data());
            // SAFETY: `out` is valid for writes. `write` doesn't drop its uninitialized contents.
            unsafe { out.write(array) };
            0
        },
        Some(Err(err)) => state.fail(err),
    }
}

unsafe extern "C" fn get_last_error(stream: *mut ArrowArrayStream) -> *const c_char {
    // SAFETY: the stream isn't released, by the C stream interface contract.
    let state = unsafe { state(stream) };
    state
        .last_error
        .as_ref()
        .map_or(std::ptr::null(), |err| err.as_ptr())
}

unsafe extern "C" fn release(stream: *mut ArrowArrayStream) {
    // SAFETY: the consumer passes a pointer to a stream it owns, which isn't released yet.
    let stream = unsafe { &mut *stream };
    // SAFETY: `new` boxed `private_data`. Clearing `release` below stops it being freed twice.
    drop(unsafe { Box::from_raw(stream.private_data.cast::<StreamState>()) });
    stream.release = None;
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_schema::extension::EXTENSION_TYPE_NAME_KEY;

    use super::*;

    fn int128_field(name: &str, extension: &str) -> Field {
        Field::new(name, DataType::FixedSizeBinary(16), true).with_metadata(HashMap::from([
            (EXTENSION_TYPE_NAME_KEY.to_owned(), extension.to_owned()),
            ("other".to_owned(), "kept".to_owned()),
        ]))
    }

    #[test]
    fn int128_export_format() {
        let schema = Schema::new(vec![
            int128_field("i", INT128_EXTENSION),
            int128_field("u", UINT128_EXTENSION),
            Field::new("plain", DataType::FixedSizeBinary(16), false),
        ]);
        let c_schema = export_schema(&schema).unwrap();

        let i = c_schema.child(0);
        assert_eq!(i.format(), "_pli128");
        assert_eq!(i.name(), Some("i"));
        assert_eq!(i.flags().map(|f| f.bits()), Some(Flags::NULLABLE.bits()));
        assert_eq!(
            i.metadata().unwrap(),
            HashMap::from([("other".to_owned(), "kept".to_owned())])
        );
        assert_eq!(c_schema.child(1).format(), "_plu128");
        assert_eq!(c_schema.child(2).format(), "w:16");
        assert_eq!(c_schema.child(2).flags().map(|f| f.bits()), Some(0));
    }

    #[test]
    fn nested_int128_export_format() {
        let item = Field::new_struct(
            "item",
            vec![
                int128_field("a", UINT128_EXTENSION),
                Field::new("b", DataType::Int32, true),
            ],
            true,
        );
        let schema = Schema::new(vec![Field::new(
            "l",
            DataType::LargeList(Arc::new(item)),
            true,
        )]);
        let c_schema = export_schema(&schema).unwrap();

        let list = c_schema.child(0);
        assert_eq!(list.format(), "+L");
        assert_eq!(list.name(), Some("l"));
        let item = list.child(0);
        assert_eq!(item.format(), "+s");
        assert_eq!(item.name(), Some("item"));
        assert_eq!(item.child(0).format(), "_plu128");
        assert_eq!(item.child(1).format(), "i");
    }
}
