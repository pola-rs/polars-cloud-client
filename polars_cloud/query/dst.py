from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from pathlib import Path
    from typing import Literal

    from polars._typing import (
        CsvQuoteStyle,
        IpcCompression,
        ParquetCompression,
        ParquetMetadata,
        PartitioningScheme,
    )
    from polars.interchange import CompatLevel
    from polars.io.cloud import CredentialProviderFunction
    from polars.io.parquet import ParquetFieldOverwrites


class Dst: ...


class ParquetDst(Dst):
    def __init__(
        self,
        uri: str | Path | PartitioningScheme,
        *,
        compression: ParquetCompression = "zstd",
        compression_level: int | None = None,
        statistics: bool | str | dict[str, bool] = True,
        row_group_size: int | None = None,
        data_page_size: int | None = None,
        maintain_order: bool = True,
        storage_options: dict[str, Any] | None = None,
        credential_provider: CredentialProviderFunction
        | Literal["auto"]
        | None = "auto",
        metadata: ParquetMetadata | None = None,
        field_overwrites: ParquetFieldOverwrites
        | Sequence[ParquetFieldOverwrites]
        | Mapping[str, ParquetFieldOverwrites]
        | None = None,
    ) -> None:
        """Parquet destination arguments.

        Parameters
        ----------
        uri
            Path to which the output should be written.
            Must be a URI to an accessible object store location.
            If set to `"local"`, the query is executed locally.
            If `None`, the result will be written to a temporary location. This
            is useful for intermediate query results.
        compression : {'lz4', 'uncompressed', 'snappy', 'gzip', 'lzo', 'brotli', 'zstd'}
            Choose "zstd" for good compression performance.
            Choose "lz4" for fast compression/decompression.
            Choose "snappy" for more backwards compatibility guarantees
            when you deal with older parquet readers.
        compression_level
            The level of compression to use. Higher compression means smaller files on
            disk.

            - "gzip" : min-level: 0, max-level: 10.
            - "brotli" : min-level: 0, max-level: 11.
            - "zstd" : min-level: 1, max-level: 22.

        statistics
            Write statistics to the parquet headers. This is the default behavior.

            Possible values:

            - `True`: enable default set of statistics (default). Some
              statistics may be disabled.
            - `False`: disable all statistics
            - "full": calculate and write all available statistics. Cannot be
              combined with `use_pyarrow`.
            - `{ "statistic-key": True / False, ... }`. Cannot be combined with
              `use_pyarrow`. Available keys:

              - "min": column minimum value (default: `True`)
              - "max": column maximum value (default: `True`)
              - "distinct_count": number of unique column values (default: `False`)
              - "null_count": number of null values in column (default: `True`)
        row_group_size
            Size of the row groups in number of rows. Defaults to 512^2 rows.
        data_page_size
            Size of the data page in bytes. Defaults to 1024^2 bytes.
        maintain_order
            Maintain the order in which data is processed.
            Setting this to `False` can be much faster.

            .. warning::
                This functionality is considered **unstable**. It may be changed at any
                point without it being considered a breaking change.
        storage_options
            Options that indicate how to connect to a cloud provider.

            The cloud providers currently supported are AWS, GCP, and Azure.
            See supported keys here:

            * `aws <https://docs.rs/object_store/latest/object_store/aws/enum.AmazonS3ConfigKey.html>`_
            * `gcp <https://docs.rs/object_store/latest/object_store/gcp/enum.GoogleConfigKey.html>`_
            * `azure <https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html>`_
            * Hugging Face (`hf://`): Accepts an API key under the `token` parameter: \
            `{'token': '...'}`, or by setting the `HF_TOKEN` environment variable.

            If `storage_options` is not provided, Polars will try to infer the
            information from environment variables.
        credential_provider
            Provide a function that can be called to provide cloud storage
            credentials. The function is expected to return a dictionary of
            credential keys along with an optional credential expiry time.

            .. warning::
                This functionality is considered **unstable**. It may be changed
                at any point without it being considered a breaking change.
        metadata
            A dictionary or callback to add key-values to the file-level Parquet
            metadata.

            .. warning::
                This functionality is considered **experimental**. It may be removed or
                changed at any point without it being considered a breaking change.
        field_overwrites
            Property overwrites for individual Parquet fields.

            This allows more control over the writing process to the granularity of a
            Parquet field.

            .. warning::
                This functionality is considered **unstable**. It may be changed
                at any point without it being considered a breaking change.

        """
        self.uri: str | Path | None | PartitioningScheme = (
            uri  #: Path to which the output should be written
        )
        self.compression: ParquetCompression = compression  #: Compression algorithm
        self.compression_level: int | None = compression_level  #: Compression level
        self.statistics: bool | str | dict[str, bool] = (
            statistics  #: Write statistics to parquet headers
        )
        self.row_group_size: int | None = row_group_size  #: Size of the row groups
        self.data_page_size: int | None = data_page_size  #: Data Page size
        self.maintain_order: bool = True
        self.storage_options: dict[str, Any] | None = (
            storage_options  #: Storage options
        )
        self.credential_provider: (
            CredentialProviderFunction | Literal["auto"] | None
        ) = credential_provider  #: Credential provider
        self.metadata: ParquetMetadata | None = metadata
        self.field_overwrites: (
            ParquetFieldOverwrites
            | Sequence[ParquetFieldOverwrites]
            | Mapping[str, ParquetFieldOverwrites]
            | None
        ) = field_overwrites


class CsvDst(Dst):
    def __init__(
        self,
        uri: str | PartitioningScheme,
        *,
        include_bom: bool = False,
        include_header: bool = True,
        separator: str = ",",
        line_terminator: str = "\n",
        quote_char: str = '"',
        batch_size: int = 1024,
        datetime_format: str | None = None,
        date_format: str | None = None,
        time_format: str | None = None,
        float_scientific: bool | None = None,
        float_precision: int | None = None,
        decimal_comma: bool = False,
        null_value: str | None = None,
        quote_style: CsvQuoteStyle | None = None,
        maintain_order: bool = True,
        storage_options: dict[str, Any] | None = None,
        credential_provider: CredentialProviderFunction
        | Literal["auto"]
        | None = "auto",
    ) -> None:
        """Csv destination arguments.

        Parameters
        ----------
        uri
            Path to which the output should be written.
            Must be a URI to an accessible object store location.

            It is recommended to write to a directory path
            for example `"my-location/"`, instead of as single file
            as a single file can only be written from a single
            node.

            If set to `"local"`, the query is executed locally.
        include_bom
            Whether to include UTF-8 BOM in the CSV output.
        include_header
            Whether to include header in the CSV output.
        separator
            Separate CSV fields with this symbol.
        line_terminator
            String used to end each row.
        quote_char
            Byte to use as quoting character.
        batch_size
            Number of rows that will be processed per thread.
        datetime_format
            A format string, with the specifiers defined by the
            `chrono <https://docs.rs/chrono/latest/chrono/format/strftime/index.html>`_
            Rust crate. If no format specified, the default fractional-second
            precision is inferred from the maximum timeunit found in the frame's
            Datetime cols (if any).
        date_format
            A format string, with the specifiers defined by the
            `chrono <https://docs.rs/chrono/latest/chrono/format/strftime/index.html>`_
            Rust crate.
        time_format
            A format string, with the specifiers defined by the
            `chrono <https://docs.rs/chrono/latest/chrono/format/strftime/index.html>`_
            Rust crate.
        float_scientific
            Whether to use scientific form always (true), never (false), or
            automatically (None) for `Float32` and `Float64` datatypes.
        float_precision
            Number of decimal places to write, applied to both `Float32` and
            `Float64` datatypes.
        decimal_comma
            Use a comma as the decimal separator instead of a point. Floats will be
            encapsulated in quotes if necessary; set the field separator to override.
        null_value
            A string representing null values (defaulting to the empty string).
        quote_style : {'necessary', 'always', 'non_numeric', 'never'}
            Determines the quoting strategy used.

            - necessary (default): This puts quotes around fields only when necessary.
              They are necessary when fields contain a quote,
              delimiter or record terminator.
              Quotes are also necessary when writing an empty record
              (which is indistinguishable from a record with one empty field).
              This is the default.
            - always: This puts quotes around every field. Always.
            - never: This never puts quotes around fields, even if that results in
              invalid CSV data (e.g.: by not quoting strings containing the
              separator).
            - non_numeric: This puts quotes around all fields that are non-numeric.
              Namely, when writing a field that does not parse as a valid float
              or integer, then quotes will be used even if they aren`t strictly
              necessary.
        maintain_order
            Maintain the order in which data is processed.
            Setting this to `False` can be much faster.

            .. warning::
                This functionality is considered **unstable**. It may be changed at any
                point without it being considered a breaking change.
        storage_options
            Options that indicate how to connect to a cloud provider.

            The cloud providers currently supported are AWS, GCP, and Azure.
            See supported keys here:

            * `aws <https://docs.rs/object_store/latest/object_store/aws/enum.AmazonS3ConfigKey.html>`_
            * `gcp <https://docs.rs/object_store/latest/object_store/gcp/enum.GoogleConfigKey.html>`_
            * `azure <https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html>`_
            * Hugging Face (`hf://`): Accepts an API key under the `token` parameter: \
            `{'token': '...'}`, or by setting the `HF_TOKEN` environment variable.

            If `storage_options` is not provided, Polars will try to infer the
            information from environment variables.
        credential_provider
            Provide a function that can be called to provide cloud storage
            credentials. The function is expected to return a dictionary of
            credential keys along with an optional credential expiry time.

            .. warning::
                This functionality is considered **unstable**. It may be changed
                at any point without it being considered a breaking change.
        """
        self.uri: str | PartitioningScheme = uri
        self.include_bom: bool = include_bom
        self.include_header: bool = include_header
        self.separator: str = separator
        self.line_terminator: str = line_terminator
        self.quote_char: str = quote_char
        self.batch_size: int = batch_size
        self.datetime_format: str | None = datetime_format
        self.date_format: str | None = date_format
        self.time_format: str | None = time_format
        self.float_scientific: bool | None = float_scientific
        self.float_precision: int | None = float_precision
        self.null_value: str | None = null_value
        self.quote_style: CsvQuoteStyle | None = quote_style
        self.maintain_order = maintain_order
        self.storage_options: dict[str, Any] | None = storage_options
        self.decimal_comma = decimal_comma
        self.credential_provider: (
            CredentialProviderFunction | Literal["auto"] | None
        ) = credential_provider


class IpcDst(Dst):
    def __init__(
        self,
        uri: str | PartitioningScheme,
        *,
        compression: IpcCompression | None = "zstd",
        compat_level: CompatLevel | None = None,
        maintain_order: bool = True,
        storage_options: dict[str, Any] | None = None,
        credential_provider: CredentialProviderFunction
        | Literal["auto"]
        | None = "auto",
    ) -> None:
        """Ipc destination arguments.

        Parameters
        ----------
        uri
            Path to which the output should be written.
            Must be a URI to an accessible object store location.

            It is recommended to write to a directory path
            for example `"my-location/"`, instead of as single file
            as a single file can only be written from a single
            node.

            If set to `"local"`, the query is executed locally.
        compression : {'uncompressed', 'lz4', 'zstd'}
            Choose "zstd" for good compression performance.
            Choose "lz4" for fast compression/decompression.
        compat_level
            Use a specific compatibility level
            when exporting Polars' internal data structures.
        maintain_order
            Maintain the order in which data is processed.
            Setting this to `False` can be much faster.

            .. warning::
                This functionality is considered **unstable**. It may be changed at any
                point without it being considered a breaking change.
        storage_options
            Options that indicate how to connect to a cloud provider.

            The cloud providers currently supported are AWS, GCP, and Azure.
            See supported keys here:

            * `aws <https://docs.rs/object_store/latest/object_store/aws/enum.AmazonS3ConfigKey.html>`_
            * `gcp <https://docs.rs/object_store/latest/object_store/gcp/enum.GoogleConfigKey.html>`_
            * `azure <https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html>`_
            * Hugging Face (`hf://`): Accepts an API key under the `token` parameter: \
            `{'token': '...'}`, or by setting the `HF_TOKEN` environment variable.

            If `storage_options` is not provided, Polars will try to infer the
            information from environment variables.
        credential_provider
            Provide a function that can be called to provide cloud storage
            credentials. The function is expected to return a dictionary of
            credential keys along with an optional credential expiry time.

            .. warning::
                This functionality is considered **unstable**. It may be changed
                at any point without it being considered a breaking change.
        """
        self.uri = uri
        self.compression = compression
        self.compat_level = compat_level
        self.maintain_order = maintain_order
        self.storage_options = storage_options
        self.credential_provider = credential_provider


class TmpDst(Dst): ...
