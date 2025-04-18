import marimo

__generated_with = "0.11.24"
app = marimo.App(width="medium")


@app.cell
def _():
    import dlt
    from dlt.sources.rest_api import rest_api_source
    return dlt, rest_api_source


@app.cell
def _(dlt):
    api_key = "?"
    dlt.secrets["api_key"] = api_key
    return (api_key,)


@app.cell
def debug_response():
    def debug_response(response, *args, **kwargs):
        print("Intercepted:", response.status_code)
        return response
    return (debug_response,)


@app.cell
def lower_title():
    def lower_title(record):
        record["title"] = record["title"].lower()
        return record
    return (lower_title,)


@app.cell
def _(api_key, debug_response, dlt, lower_title, rest_api_source):
    # Define config
    news_config = {
        "client": {
            "base_url": "https://newsapi.org/v2/",
            "auth": {
                "type": "api_key",
                "name": "apiKey",
                "api_key": api_key,
                "location": "query",
            },
            "paginator": {
                "base_page": 1,
                "type": "page_number",
                "page_param": "page",
                "total_path": None,
                "maximum_page": 3,
            },
        },
        "resource_defaults": {
            "write_disposition": "append",
            "endpoint": {
                "params": {
                    "language": "en",
                    "pageSize": 20,
                },
            },
        },
        "resources": [
            {
                "name": "news_articles",
                "processing_steps": [
                        # {"filter": lambda x: len(x["author"]) > 0}, # <--- add filter
                        {"map": lower_title}, # <--- add some transformation
                    ],
                "endpoint": {
                    "path": "everything",
                    "response_actions": [
                        {
                            "status_code": 200,
                            "action": debug_response, # <--- add some action
                        },
                    ],
                    "params": {
                        "q": "python",
                        # "language": "en",
                        # "pageSize": 20,
                        "from": {
                            "type": "incremental",
                            "cursor_path": "publishedAt",
                            "initial_value": "2025-04-15T00:00:00Z",
                        },
                    }
                },
            },
            {
                "name": "top_headlines",
                "endpoint": {
                    "path": "top-headlines",
                    "params": {"country": "us"},
                },

            }
        ]
    }

    # Create source
    news_source = rest_api_source(news_config)

    # Create pipeline
    pipeline = dlt.pipeline(
      pipeline_name="news_pipeline",
      destination="duckdb",
      dataset_name="news"
    )

    # Run it
    load_info = pipeline.run(news_source)
    print(load_info)
    print(pipeline.last_trace)
    return load_info, news_config, news_source, pipeline


@app.cell
def _(pipeline):
    pipeline.dataset(dataset_type="default").news_articles.df().head()
    return


if __name__ == "__main__":
    app.run()
