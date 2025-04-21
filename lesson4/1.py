

import marimo

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import dlt
    from dlt.common.typing import TDataItems
    from dlt.common.schema import TTableSchema

    import os
    from dlt.sources.sql_database import sql_database
    import sqlalchemy as sa
    from sqlalchemy import text

    from notion_client import Client
    return Client, TDataItems, TTableSchema, dlt, os, sql_database, text


@app.cell
def _(TDataItems, TTableSchema, dlt):
    @dlt.destination(batch_size=5)
    def print_sink(items: TDataItems, table: TTableSchema):
        print(f"\nTable: {table['name']}")
        for item in items:
            print(item)


    @dlt.resource
    def simple_data():
        yield [{"id": i, "value": f"row-{i}"} for i in range(12)]


    pipeline1 = dlt.pipeline("print_example", destination=print_sink)
    pipeline1.run(simple_data())
    print(pipeline1.last_trace)
    return


@app.cell
def _(sql_database, text):

    def limit_rows(query, table):
        return text(f"SELECT * FROM {table.fullname} LIMIT 20")


    source = sql_database(
        "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam",
        table_names=["family",],
        query_adapter_callback=limit_rows
    )
    return (source,)


@app.cell
def _(Client, dlt, os):
    os.environ["DESTINATION__NOTION__NOTION_AUTH"] = "?"
    os.environ["DESTINATION__NOTION__NOTION_PAGE_ID"] = "?"


    @dlt.destination(name="notion")
    def push_to_notion(items, table, notion_auth=dlt.secrets.value, notion_page_id=dlt.secrets.value):
        client = Client(auth=notion_auth)
        print(len(items))
        for item in items:
            client.pages.create(
                parent={"database_id": notion_page_id},
                properties={
                    "Accession": {"title": [{"text": {"content": item["rfam_acc"]}}]},
                    "ID": {"rich_text": [{"text": {"content": item["rfam_id"]}}]},
                    "Description": {"rich_text": [{"text": {"content": item["description"]}}]}
                }
            )
    return (push_to_notion,)


@app.cell
def _(dlt, push_to_notion, source):
    pipeline = dlt.pipeline("notion_pipeline", destination=push_to_notion, progress="log")
    pipeline.run(source, table_name="rfam_family")
    print(pipeline.last_trace)
    return


if __name__ == "__main__":
    app.run()
