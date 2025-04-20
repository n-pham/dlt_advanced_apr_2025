

import marimo

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import dlt
    from dlt.sources.rest_api import rest_api_source
    return dlt, rest_api_source


@app.cell
def _():
    # Define config
    orders_config = {
        "client": {
            "base_url": "https://jaffle-shop.scalevector.ai/api/v1",
            "paginator": {
                "base_page": 1,
                "type": "page_number",
                "page_param": "page",
                "total_path": None,
                # "maximum_page": 3, # test
            },
        },
        "resource_defaults": {
            "write_disposition": "append",
            "endpoint": {
                "params": {
                    "page_size": 100,
                },
            },
        },
        "resources": [
            {
                "name": "orders",
                "processing_steps": [
                        {"filter": lambda x: int(x["order_total"]) > 500},
                    ],
                "endpoint": {
                    "path": "orders",
                    "params": {
                        "start_date": {
                            "type": "incremental",
                            "cursor_path": "ordered_at",
                            "initial_value": "2017-08-01",
                        },
                    }
                },
            }
        ]
    }

    return (orders_config,)


@app.cell
def _(dlt, orders_config, rest_api_source):

    # Create source
    orders_source = rest_api_source(orders_config)

    # Create pipeline
    pipeline = dlt.pipeline(
      pipeline_name="orders_pipeline",
      destination="duckdb",
      dataset_name="orders"
    )

    # Run it
    load_info = pipeline.run(orders_source)
    print(load_info)
    print(pipeline.last_trace)
    return (pipeline,)


@app.cell
def _(pipeline):
    pipeline.dataset(dataset_type="default").orders.df()
    return


if __name__ == "__main__":
    app.run()
