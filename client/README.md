![Image showing the Polars Cloud logo](https://raw.githubusercontent.com/pola-rs/polars-static/ce84036de7e939a82eb57c88056244c17f35fdba/logos/polars_cloud_logo_blue.svg)

<div align="center">
  <a href="https://pypi.org/project/polars_cloud/">
    <img src="https://img.shields.io/pypi/v/polars_cloud.svg" alt="PyPi Latest Release"/>
  </a>

<a href="https://docs.cloud.pola.rs/reference/index.html">Documentation</a>
|
<a href="https://stackoverflow.com/questions/tagged/polars-cloud">Stack Overflow</a>
|
<a href="https://docs.pola.rs/polars-cloud/">User guide</a>
|
<a href="https://discord.gg/4UfP5cfBE7">Discord</a>

</div>

# Polars Cloud: Run your queries at scale, anywhere

Built on top of the popular open source project, Polars Cloud enables you to write DataFrame code
once and run it anywhere. The distributed engine available with Polars Cloud allows you to scale
your Polars queries beyond a single machine.

## Key Features of Polars Cloud

- **Unified DataFrame Experience**: Run a Polars query seamlessly on your local machine or at scale with our new
  distributed engine. All from the same API.
- **Serverless Compute**: Effortlessly start compute resources without managing infrastructure, with options to run
  queries on both CPU and GPU.
- **Any Environment**: Start a remote query from a notebook on your machine, Airflow DAG, AWS Lambda, or any server.
  Get the flexibility to embed Polars Cloud in any environment.

## Install Polars Cloud

To use Polars cloud simply add it to your existing project

```bash
pip install polars_cloud
```

Then call `.remote()` on your dataframe and provide a compute context.

```python
import polars as pl
import polars_cloud as pc

ctx = pc.ComputeContext(cpus=16, memory=64)

query = (
    pl.scan_parquet("s3://my-dataset/")
    .group_by("returned", "status")
    .agg(
        avg_price=pl.mean("price"),
        avg_disc=pl.mean("discount"),
        count_order=pl.len(),
    )
)

(
    query.remote(ctx)
    .distributed()
    .sink_parquet("s3://my-destination/")
)
```

Hit run and your query will be executed in the cloud. You can follow your query's progress
on [the dashboard](https://cloud.pola.rs/portal/dashboard). And once your first query is done it's time to increase your
dataset size and up the core count.

## Sign up today

[Sign up here](https://cloud.pola.rs/) to run Polars Cloud.
