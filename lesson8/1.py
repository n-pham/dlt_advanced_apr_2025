

import marimo

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import dlt
    from dlt.sources.helpers import requests
    from dlt.sources.helpers.rest_client import RESTClient
    from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
    from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator
    import os
    import duckdb
    return BearerTokenAuth, HeaderLinkPaginator, RESTClient, dlt, duckdb, os


@app.cell
def _(os):
    os.environ["SOURCES__SECRET_KEY"] = "?"
    return


@app.cell
def _(BearerTokenAuth, HeaderLinkPaginator, RESTClient, dlt):
    @dlt.source
    def github_source(secret_key=dlt.secrets.value):
        client = RESTClient(
                base_url="https://api.github.com",
                auth=BearerTokenAuth(token=secret_key),
                paginator=HeaderLinkPaginator(),
        )

        @dlt.resource
        def github_pulls(cursor_date=dlt.sources.incremental("updated_at", initial_value="2024-12-01")):
            params = {
                "since": cursor_date.last_value,
                "status": "open"
            }
            for page in client.paginate("repos/n-pham/dsa/pulls", params=params):
                yield page


        return github_pulls

    return (github_source,)


@app.cell
def _(dlt, duckdb):
    # define new dlt pipeline
    pipeline = dlt.pipeline(
        pipeline_name="github_pipeline",
        destination="duckdb",
        dataset_name="github_data",
    )

    conn = duckdb.connect(f"{pipeline.pipeline_name}.duckdb")
    

    return conn, pipeline


@app.cell
def _(conn, github_source, pipeline):
    def _():
        # run the pipeline with the new resource
        load_info = pipeline.run(github_source())
        print(load_info)
        print(conn.sql("SHOW ALL TABLES").df())
        print(conn.sql("select * from github_data._dlt_loads").df())
    _()
    return


if __name__ == "__main__":
    app.run()
