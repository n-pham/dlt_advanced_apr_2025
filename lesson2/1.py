

import marimo

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _():
    from dlt.sources.sql_database import sql_database
    import dlt
    from sqlalchemy import text
    import os
    from sqlalchemy.sql import sqltypes
    import sqlalchemy as sa
    from dlt.sources.sql_database import sql_table
    import json
    import pendulum
    return dlt, json, os, pendulum, sa, sql_database, sql_table, text


@app.cell
def _(os):
    os.environ["SOURCES__SQL_DATABASE__CREDENTIALS"] = "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam"
    return


@app.cell
def _(dlt, sql_database):
    source = sql_database(
        "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam",
        table_names=["family"]
    )

    pipeline = dlt.pipeline(
        pipeline_name="sql_database_example",
        destination="duckdb",
        dataset_name="sql_data",
        dev_mode=True,
    )

    load_info = pipeline.run(source)
    print(load_info)
    return (pipeline,)


@app.cell
def _(pipeline):
    pipeline.dataset().family.df().head()
    return


@app.cell
def _(text):
    def query_adapter_callback(query, table, incremental=None, engine=None):
        return text(f"SELECT * FROM {table.fullname} WHERE rfam_id like '%bacteria%'")
    return (query_adapter_callback,)


@app.cell
def _(pipeline, query_adapter_callback, sql_database):
    def _():
        filtered_resource = sql_database(
            query_adapter_callback=query_adapter_callback,
            table_names=["family"]
        )
        info = pipeline.run(filtered_resource, table_name="bacterias")
        print(info)
    return


@app.cell
def _(pipeline):
    pipeline.dataset().bacterias.df().head()
    return


@app.cell
def _(pipeline):
    pipeline.dataset().bacterias.df().count()
    return


@app.cell
def _(dlt, pipeline, sa, sql_table):
    def add_max_timestamp(table):
        max_ts = sa.func.greatest(table.c.created, table.c.updated).label("max_timestamp")
        subq = sa.select(*table.c, max_ts).subquery()
        return subq

    table = sql_table(
        table="family",
        table_adapter_callback=add_max_timestamp,
        incremental=dlt.sources.incremental("max_timestamp")
    )

    info = pipeline.run(table, table_name="family_with_max_timestamp")
    print(info)
    return


@app.cell
def _(pipeline):
    pipeline.dataset().family_with_max_timestamp.df().head()
    return


@app.cell
def _(pipeline):
    schema = pipeline.default_schema.to_dict()["tables"]["family"]["columns"]
    for column_t in schema:
      print(schema[column_t]["name"], ":", schema[column_t]["data_type"])
    return


@app.cell
def _(sa):
    def type_adapter_callback(sql_type):
      if isinstance(sql_type, sa.Numeric):
            return sa.Double
      return sql_type
    return (type_adapter_callback,)


@app.cell
def _(pipeline, sql_database, type_adapter_callback):
    new_source = sql_database(
        type_adapter_callback=type_adapter_callback,
        table_names=["family"]
    )

    info2 = pipeline.run(new_source, table_name="type_changed_family")
    print(info2)
    return


@app.cell
def _(pipeline):
    schema1 = pipeline.default_schema.to_dict()["tables"]["family"]["columns"]
    schema2 = pipeline.default_schema.to_dict()["tables"]["type_changed_family"]["columns"]
    column = "trusted_cutoff"

    print("For table 'family':", schema1[column]["name"], ":", schema1[column]["data_type"])
    print("For table 'type_changed_family':", schema2[column]["name"], ":", schema2[column]["data_type"])
    return


@app.cell
def _(json):
    with open("/var/dlt/pipelines/sql_database_example/state.json", "r") as f:
      data = json.load(f)

    data["sources"]["sql_database"]["resources"]["family"]["incremental"].keys()
    return


@app.cell
def _(dlt, pendulum, pipeline, sql_database):
    source3 = sql_database().with_resources("family")
    source3.family.apply_hints(
        incremental=dlt.sources.incremental("updated", initial_value=pendulum.datetime(2024, 1, 1))
    )

    info3 = pipeline.run(source3)
    print(info3)
    return


@app.cell
def _(pipeline):
    pipeline.dataset().family.df().head()
    return


if __name__ == "__main__":
    app.run()
