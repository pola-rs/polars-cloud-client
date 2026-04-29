use proc_macro::TokenStream;
use quote::quote;
use semver::Version;
use syn::{Attribute, Item, LitStr, parse_macro_input};

#[proc_macro_attribute]
pub fn deprecated_since_client(attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut input_item = parse_macro_input!(item as Item);
    let mut errors: Vec<proc_macro2::TokenStream> = Vec::new();

    let min_version = Version::parse("0.4.3").expect("Invalid min version");
    if !attr.is_empty() {
        let attr_lit = parse_macro_input!(attr as LitStr);
        let attr_version = Version::parse(&attr_lit.value()).expect("Invalid version");
        if min_version > attr_version {
            let msg = format!(
                "Item marked deprecated since client v{attr_version}. Current minimum supported client is v{min_version}."
            );
            errors.push(syn::Error::new(attr_lit.span(), msg).to_compile_error());
        }
    }

    match &mut input_item {
        Item::Struct(s) => {
            for field in s.fields.iter_mut() {
                process_attribute(
                    &mut field.attrs,
                    field
                        .ident
                        .as_ref()
                        .map(|x: &syn::Ident| x.to_string())
                        .unwrap_or("struct".into()),
                    &min_version,
                    &mut errors,
                );
            }
        },
        Item::Enum(e) => {
            for variant in e.variants.iter_mut() {
                process_attribute(
                    &mut variant.attrs,
                    variant.ident.to_string(),
                    &min_version,
                    &mut errors,
                )
            }
        },
        _ => {},
    }

    quote! {
        #(#errors)*
        #input_item
    }
    .into()
}

fn process_attribute(
    attributes: &mut Vec<Attribute>,
    name: String,
    min_version: &Version,
    errors: &mut Vec<proc_macro2::TokenStream>,
) {
    attributes.retain(|attr| {
        if !attr.path().is_ident("deprecated_since_client") {
            return true;
        }

        let Ok(literal) = attr.parse_args::<LitStr>() else {
            return false;
        };

        let version = Version::parse(&literal.value()).unwrap();
        if *min_version >= version {
            let msg = format!(
                "Item '{name}' marked deprecated since client v{version}. Current minimum supported client is v{min_version}.",
            );
            errors.push(syn::Error::new(literal.span(), msg).to_compile_error());
        }

        false
    });
}
