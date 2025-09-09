from __future__ import annotations

import copy
from typing import TYPE_CHECKING, Any

import polars as pl
from polars.lazyframe.opt_flags import DEFAULT_QUERY_OPT_FLAGS

from polars_cloud.query.dst import CsvDst, IpcDst, ParquetDst, TmpDst
from polars_cloud.query.query import DistributionSettings, spawn

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from typing import Literal

    from polars import DataFrame, QueryOptFlags
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

    from polars_cloud._typing import (
        Engine,
        PlanTypePreference,
        ShuffleCompression,
        ShuffleFormat,
    )
    from polars_cloud.context import ComputeContext
    from polars_cloud.query.query import DirectQuery, ProxyQuery


class LazyFrameRemote:
    """The namespace accessed by `LazyFrame.remote`.

    This will allow you to run a query remotely.
    """

    def __init__(
        self,
        lf: pl.LazyFrame,
        context: ComputeContext | None = None,
        plan_type: PlanTypePreference = "dot",
        n_retries: int = 0,
        engine: Engine = "auto",
    ) -> None:
        self.lf: pl.LazyFrame = lf
        self.context: ComputeContext | None = context
        self._partition_by: None | str | list[str] = None
        self._engine: Engine = engine
        self._labels: None | list[str] = None
        self._n_retries = n_retries
        self.plan_type: PlanTypePreference = plan_type

    def distributed(
        self,
        *,
        shuffle_compression: ShuffleCompression = "auto",
        shuffle_format: ShuffleFormat = "auto",
        sort_partitioned: bool = True,
        pre_aggregation: bool = True,
    ) -> ExecuteRemote:
        """Whether the query should run in a distributed fashion.

        Parameters
        ----------
        shuffle_compression : {'auto', 'lz4', 'zstd', 'uncompressed'}
            Compress files before shuffling them. Compression reduces disk and network
            IO, but disables memory mapping.
            Choose "zstd" for good compression performance.
            Choose "lz4" for fast compression/decompression.
            Choose "uncompressed" for memory mapped access at the expense of file size.
        shuffle_format : {'auto', 'ipc', 'parquet'}
            File format to use for shuffles.
        sort_partitioned
            Whether group-by and selected aggregations are pre-aggregated
            on worker nodes.
        pre_aggregation
            Whether group-by and selected aggregations are pre-aggregated on
            worker nodes if possible.

        Examples
        --------
        >>> ctx = pc.ComputeContext(cluster_size=10)
        >>> query.remote(ctx).distributed().sink_parquet(...)
        """
        distributed_settings = DistributionSettings(
            sort_partitioned=sort_partitioned, pre_aggregation=pre_aggregation
        )
        exec = ExecuteRemote(
            lf=self.lf,
            context=self.context,
            plan_type=self.plan_type,
            n_retries=self._n_retries,
            labels=self._labels,
            engine=self._engine,
            distributed_settings=distributed_settings,
            shuffle_compression=shuffle_compression,
            shuffle_format=shuffle_format,
        )

        return exec

    def labels(self, labels: list[str] | str) -> LazyFrameRemote:
        """Add labels to the query.

        Parameters
        ----------
        labels
            Labels to add to the query (will be implicitly created)

        Examples
        --------
        >>> query.remote(ctx).labels("docs").sink_parquet(...)
        """
        self._labels = [labels] if isinstance(labels, str) else labels
        return self

    def partition_by(
        self,
        key: str | list[str],
        *,
        shuffle_compression: ShuffleCompression = "auto",
        shuffle_format: ShuffleFormat = "auto",
    ) -> ExecuteRemote:
        """Partition this query by the given key.

        This first partitions the data by the key and then runs this query
        per unique key. This will lead to ``N`` output results, where ``N``
        is equal to the number of unique values in ``key``

        This will run on multiple workers.

        Parameters
        ----------
        key
            Key/keys to partition over.
        shuffle_compression : {'auto', 'lz4', 'zstd', 'uncompressed'}
            Compress files before shuffling them. Compression reduces disk and network
            IO, but disables memory mapping.
            Choose "zstd" for good compression performance.
            Choose "lz4" for fast compression/decompression.
            Choose "uncompressed" for memory mapped access at the expense of file size.
        shuffle_format : {'auto', 'ipc', 'parquet'}
            File format to use for shuffles.
        """
        exec = ExecuteRemote(
            lf=self.lf,
            context=self.context,
            plan_type=self.plan_type,
            n_retries=self._n_retries,
            labels=self._labels,
            engine=self._engine,
            partition_by=key,
            shuffle_compression=shuffle_compression,
            shuffle_format=shuffle_format,
        )

        return exec

    def vertical(self) -> ExecuteRemote:
        """Run this query remotely on a single node."""
        return ExecuteRemote(
            lf=self.lf,
            context=self.context,
            plan_type=self.plan_type,
            n_retries=self._n_retries,
            labels=self._labels,
            engine=self._engine,
        )

    def execute(self) -> DirectQuery | ProxyQuery:
        """Start executing the query and store an intermediate result.

        This is useful for direct connect workloads to cache the results of a query.

        Examples
        --------
        >>> result = query.remote(ctx).execute().await_result()
        >>> intermediate_lf = result.lazy()

        See Also
        --------
        await_and_scan: Start executing the query and store a temporary result.
        """
        return self.vertical().execute()

    def await_and_scan(self) -> pl.LazyFrame:
        """Start executing the query and store a temporary result.

        This will immediately block this thread and wait for
        a successful result. It will immediately turn the result
        into a `LazyFrame`.

        This is syntactic sugar for:

        ``.execute().await_result().lazy()``

        Examples
        --------
        >>> query.remote(ctx).await_and_scan()
        NAIVE QUERY PLAN
        run LazyFrame.show_graph() to see the optimized version
        Parquet SCAN [https://s3.eu-west-1.amazonaws.com/polars-cloud-xxxxxxx-xxxx-..]
        """
        return self.vertical().await_and_scan()

    def show(self, n: int = 10) -> DataFrame:
        """Start executing the query return the first `n` rows.

        Show will immediately block this thread and wait for
        a successful result. It will immediately turn the result
        into a `DataFrame`.

        Parameters
        ----------
        n
            Number of rows to return

        Examples
        --------
        >>> pl.scan_parquet("s3://..").select(
        ...     pl.len()
        ... ).remote().show()  # doctest: +SKIP
        shape: (1, 1)
        ┌───────┐
        │ count │
        │ ---   │
        │ u32   │
        ╞═══════╡
        │ 1000  │
        └───────┘

        """
        return self.vertical().show()

    def sink_parquet(
        self,
        uri: str | PartitioningScheme,
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
        optimizations: QueryOptFlags = DEFAULT_QUERY_OPT_FLAGS,
    ) -> DirectQuery | ProxyQuery:
        """Start executing the query and write the result to parquet.

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
        optimizations
            The optimization passes done during query optimization.

            .. warning::
                This functionality is considered **unstable**. It may be changed
                at any point without it being considered a breaking change.

        Examples
        --------
        >>> query.remote(ctx).sink_parquet("s3://your-bucket/folder/file.parquet")
        <polars_cloud.query.query.ProxyQuery at 0x109ca47d0>
        """
        return self.vertical().sink_parquet(
            uri=uri,
            compression=compression,
            compression_level=compression_level,
            statistics=statistics,
            row_group_size=row_group_size,
            data_page_size=data_page_size,
            maintain_order=maintain_order,
            storage_options=storage_options,
            credential_provider=credential_provider,
            metadata=metadata,
            field_overwrites=field_overwrites,
            optimizations=optimizations,
        )

    def sink_csv(
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
        storage_options: dict[str, Any] | None = None,
        credential_provider: CredentialProviderFunction
        | Literal["auto"]
        | None = "auto",
    ) -> DirectQuery | ProxyQuery:
        """Start executing the query and write the result to csv.

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

        Examples
        --------
        >>> query.remote(ctx).sink_csv("s3://your-bucket/folder/file.csv")
        <polars_cloud.query.query.ProxyQuery at 0x107e68fb0>
        """
        return self.vertical().sink_csv(
            uri=uri,
            include_bom=include_bom,
            include_header=include_header,
            separator=separator,
            line_terminator=line_terminator,
            quote_char=quote_char,
            batch_size=batch_size,
            datetime_format=datetime_format,
            date_format=datetime_format,
            time_format=time_format,
            float_scientific=float_scientific,
            float_precision=float_precision,
            decimal_comma=decimal_comma,
            null_value=null_value,
            quote_style=quote_style,
            storage_options=storage_options,
            credential_provider=credential_provider,
        )

    def sink_ipc(
        self,
        uri: str | PartitioningScheme,
        *,
        compression: IpcCompression | None = "zstd",
        compat_level: CompatLevel | None = None,
        storage_options: dict[str, Any] | None = None,
        credential_provider: CredentialProviderFunction
        | Literal["auto"]
        | None = "auto",
    ) -> DirectQuery | ProxyQuery:
        """Start executing the query and write the result to ipc.

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

        Examples
        --------
        >>> query.remote(ctx).sink_ipc("s3://your-bucket/folder/file.ipc")
        <polars_cloud.query.query.ProxyQuery at 0x10a0a4110>
        """
        return self.vertical().sink_ipc(
            uri=uri,
            compression=compression,
            compat_level=compat_level,
            storage_options=storage_options,
            credential_provider=credential_provider,
        )


class ExecuteRemote:
    """The namespace accessed by choosing a remote execution method in a `LazyFrameRemote`."""  # noqa: W505

    def __init__(
        self,
        lf: pl.LazyFrame,
        context: ComputeContext | None,
        plan_type: PlanTypePreference,
        n_retries: int,
        engine: Engine,
        labels: list[str] | None,
        partition_by: None | str | list[str] = None,
        shuffle_compression: ShuffleCompression = "auto",
        shuffle_format: ShuffleFormat = "auto",
        distributed_settings: DistributionSettings | None = None,
    ) -> None:
        self.lf: pl.LazyFrame = lf
        self.context: ComputeContext | None = context
        self._partition_by: None | str | list[str] = partition_by
        self._engine: Engine = engine
        self._labels: None | list[str] = labels
        self._n_retries = n_retries
        self.plan_type: PlanTypePreference = plan_type
        # Optimizations settings for distributed
        self._shuffle_compression: ShuffleCompression = shuffle_compression
        self._shuffle_format: ShuffleFormat = shuffle_format
        self._distributed_settings: DistributionSettings | None = distributed_settings

    def execute(self) -> DirectQuery | ProxyQuery:
        """Start executing the query and store an intermediate result.

        This is useful for direct connect workloads to cache the results of a query.

        Examples
        --------
        >>> result = query.remote(ctx).execute().await_result()
        >>> intermediate_lf = result.lazy()

        See Also
        --------
        await_and_scan: Start executing the query and store a temporary result.
        """
        return spawn(
            lf=self.lf,
            dst=TmpDst(),
            context=self.context,
            partitioned_by=self._partition_by,
            engine=self._engine,
            plan_type=self.plan_type,
            labels=self._labels,
            shuffle_compression=self._shuffle_compression,
            shuffle_format=self._shuffle_format,
            n_retries=self._n_retries,
            distributed=self._distributed_settings,
            optimizations=pl.QueryOptFlags(),
        )

    def await_and_scan(self) -> pl.LazyFrame:
        """Start executing the query and store a temporary result.

        This will immediately block this thread and wait for
        a successful result. It will immediately turn the result
        into a `LazyFrame`.

        This is syntactic sugar for:

        ``.execute().await_result().lazy()``

        Examples
        --------
        >>> query.remote(ctx).await_and_scan()
        NAIVE QUERY PLAN
        run LazyFrame.show_graph() to see the optimized version
        Parquet SCAN [https://s3.eu-west-1.amazonaws.com/polars-cloud-xxxxxxx-xxxx-..]
        """
        return self.execute().await_result().lazy()

    def show(self, n: int = 10) -> DataFrame:
        """Start executing the query return the first `n` rows.

        Show will immediately block this thread and wait for
        a successful result. It will immediately turn the result
        into a `DataFrame`.

        Parameters
        ----------
        n
            Number of rows to return

        Examples
        --------
        >>> pl.scan_parquet("s3://..").select(
        ...     pl.len()
        ... ).remote().show()  # doctest: +SKIP
        shape: (1, 1)
        ┌───────┐
        │ count │
        │ ---   │
        │ u32   │
        ╞═══════╡
        │ 1000  │
        └───────┘

        """
        this = copy.copy(self)
        this.lf = this.lf.limit(n)
        return this.await_and_scan().collect()

    def sink_parquet(
        self,
        uri: str | PartitioningScheme,
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
        sink_to_single_file: None | bool = None,
        optimizations: QueryOptFlags = DEFAULT_QUERY_OPT_FLAGS,
    ) -> DirectQuery | ProxyQuery:
        """Start executing the query and write the result to parquet.

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
        sink_to_single_file
            Perform the sink into a single file.

            Setting this to `True` can reduce the amount of work that can be done in a
            distributed manner and therefore be more memory intensive and
            slower.
        optimizations
            The optimization passes done during query optimization.

            .. warning::
                This functionality is considered **unstable**. It may be changed
                at any point without it being considered a breaking change.

        Examples
        --------
        >>> query.remote(ctx).sink_parquet("s3://your-bucket/folder/file.parquet")
        <polars_cloud.query.query.ProxyQuery at 0x109ca47d0>
        """
        dst = ParquetDst(
            uri=uri,
            compression=compression,
            compression_level=compression_level,
            statistics=statistics,
            row_group_size=row_group_size,
            data_page_size=data_page_size,
            storage_options=storage_options,
            credential_provider=credential_provider,
            metadata=metadata,
            field_overwrites=field_overwrites,
        )

        return spawn(
            lf=self.lf,
            dst=dst,
            sink_to_single_file=sink_to_single_file,
            context=self.context,
            partitioned_by=self._partition_by,
            engine=self._engine,
            plan_type=self.plan_type,
            labels=self._labels,
            shuffle_compression=self._shuffle_compression,
            n_retries=self._n_retries,
            distributed=self._distributed_settings,
            optimizations=optimizations,
        )

    def sink_csv(
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
        sink_to_single_file: bool | None = None,
        optimizations: QueryOptFlags = DEFAULT_QUERY_OPT_FLAGS,
    ) -> DirectQuery | ProxyQuery:
        """Start executing the query and write the result to csv.

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
        sink_to_single_file
            Perform the sink into a single file.

            Setting this to `True` can reduce the amount of work that can be done in a
            distributed manner and therefore be more memory intensive and
            slower.
        optimizations
            The optimization passes done during query optimization.

            .. warning::
                This functionality is considered **unstable**. It may be changed
                at any point without it being considered a breaking change.

        Examples
        --------
        >>> query.remote(ctx).sink_csv("s3://your-bucket/folder/file.csv")
        <polars_cloud.query.query.ProxyQuery at 0x107e68fb0>
        """
        dst = CsvDst(
            uri,
            include_bom=include_bom,
            include_header=include_header,
            separator=separator,
            line_terminator=line_terminator,
            quote_char=quote_char,
            batch_size=batch_size,
            datetime_format=datetime_format,
            date_format=date_format,
            time_format=time_format,
            float_scientific=float_scientific,
            float_precision=float_precision,
            null_value=null_value,
            quote_style=quote_style,
            maintain_order=maintain_order,
            storage_options=storage_options,
            credential_provider=credential_provider,
            decimal_comma=decimal_comma,
        )
        return spawn(
            lf=self.lf,
            dst=dst,
            context=self.context,
            partitioned_by=self._partition_by,
            engine=self._engine,
            plan_type=self.plan_type,
            labels=self._labels,
            shuffle_compression=self._shuffle_compression,
            shuffle_format=self._shuffle_format,
            n_retries=self._n_retries,
            distributed=self._distributed_settings,
            sink_to_single_file=sink_to_single_file,
            optimizations=optimizations,
        )

    def sink_ipc(
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
        sink_to_single_file: bool | None = None,
        optimizations: QueryOptFlags = DEFAULT_QUERY_OPT_FLAGS,
    ) -> DirectQuery | ProxyQuery:
        """Start executing the query and write the result to ipc.

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
        sink_to_single_file
            Perform the sink into a single file.

            Setting this to `True` can reduce the amount of work that can be done in a
            distributed manner and therefore be more memory intensive and
            slower.
        optimizations
            The optimization passes done during query optimization.

            .. warning::
                This functionality is considered **unstable**. It may be changed
                at any point without it being considered a breaking change.

        Examples
        --------
        >>> query.remote(ctx).sink_ipc("s3://your-bucket/folder/file.ipc")
        <polars_cloud.query.query.ProxyQuery at 0x10a0a4110>
        """
        dst = IpcDst(
            uri,
            compression=compression,
            compat_level=compat_level,
            maintain_order=maintain_order,
            storage_options=storage_options,
            credential_provider=credential_provider,
        )
        return spawn(
            lf=self.lf,
            dst=dst,
            context=self.context,
            partitioned_by=self._partition_by,
            engine=self._engine,
            plan_type=self.plan_type,
            labels=self._labels,
            shuffle_compression=self._shuffle_compression,
            shuffle_format=self._shuffle_format,
            n_retries=self._n_retries,
            distributed=self._distributed_settings,
            sink_to_single_file=sink_to_single_file,
            optimizations=optimizations,
        )


def _lf_remote(
    lf: pl.LazyFrame,
    context: ComputeContext | None = None,
    *,
    plan_type: PlanTypePreference = "dot",
    n_retries: int = 0,
    engine: Engine = "auto",
) -> LazyFrameRemote:
    return LazyFrameRemote(
        lf, context=context, plan_type=plan_type, n_retries=n_retries, engine=engine
    )


# Overwrite the remote method, so that we are sure we already expose
# the latest arguments.
pl.LazyFrame.remote = _lf_remote  # type: ignore[method-assign, assignment]
