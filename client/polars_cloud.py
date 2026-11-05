msg = (
    "`polars_cloud` is renamed to `polars_nanook`: use `import polars_nanook as pn`, "
    "or install the deprecated `polars-cloud` during the transition"
)
raise ModuleNotFoundError(msg, name="polars_cloud")
