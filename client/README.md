![Image showing the Polars logo](https://raw.githubusercontent.com/pola-rs/polars-static/master/banner/polars_github_banner.svg)

<div align="center">
  <a href="https://pypi.org/project/polars-nanook/">
    <img src="https://img.shields.io/pypi/v/polars-nanook.svg" alt="PyPi Latest Release"/>
  </a>

<a href="https://docs.nanook.pola.rs/api/python/stable/reference/index.html">Documentation</a>
|
<a href="https://docs.nanook.pola.rs/">User guide</a>
|
<a href="https://discord.gg/4UfP5cfBE7">Discord</a>

</div>

# Polars Nanook: Run your queries at scale, anywhere

Polars Nanook is the compute platform for Polars. The queries you write and test on your laptop
run in production exactly as they are, with no rewrite and no new API. When your data outgrows one
machine, the distributed engine scales the same query across a full cluster. It runs on AWS,
Kubernetes or bare metal, so your data never leaves your environment.

## Key Features of Polars Nanook

- **Write once, run anywhere**: The Polars code you run locally runs in production unchanged,
  whether you write it with the DataFrame API or in SQL. You don't rewrite it for another
  framework, you just call `.remote()`.
- **Scale without complexity**: Go from a single machine to a distributed cluster when your data
  needs it. Choose CPUs, memory or GPUs in a `ComputeContext` and Nanook handles provisioning.
- **Runs next to your data**: Deploy on AWS, Kubernetes or bare metal, including air-gapped
  environments. Your data never leaves your infrastructure.
- **Run from any Python environment**: Submit queries from a notebook, an Airflow DAG, a Lambda
  function or any server. It's a `pip install`, not a platform migration.
- **Profile and optimize**: See how every query ran with the built-in query profiler, or let your
  agents inspect queries and suggest speedups through the MCP server.

## Install Polars Nanook

To use Polars Nanook simply add it to your existing project

```bash
pip install polars-nanook
```

Then call `.remote()` on your dataframe and provide a compute context.

```python
import polars as pl
import polars_nanook as pn

ctx = pn.ComputeContext(cpus=16, memory=64)

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
on [the dashboard](https://nanook.pola.rs/portal/dashboard). And once your first query is done it's time to increase your
dataset size and up the core count.

## Sign up today

[Sign up here](https://nanook.pola.rs/portal) to run Polars Nanook.
