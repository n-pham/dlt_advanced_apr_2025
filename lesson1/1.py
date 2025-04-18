import marimo

__generated_with = "0.11.24"
app = marimo.App(width="medium")


@app.cell
def _():
    import os
    import dlt
    from dlt.sources.helpers.rest_client import RESTClient
    from dlt.sources.helpers.rest_client.auth import APIKeyAuth
    from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
    return APIKeyAuth, PageNumberPaginator, RESTClient, dlt, os


@app.cell
def _(dlt):
    api_key = "?"
    dlt.secrets["api_key"] = api_key
    return (api_key,)


@app.cell
def _(APIKeyAuth, RESTClient, api_key):
    client = RESTClient(
        base_url="https://newsapi.org/v2/",
        auth=APIKeyAuth(name="apiKey", api_key=api_key, location="query")
    )

    response = client.get("everything", params={"q": "python", "page": 1})
    print(response.json())
    return client, response


@app.cell
def _(client):
    def _():
        # guess paging method

        response = client.paginate("everything", params={"q": "python", "page": 1})
        # prints the original request object
        print(next(response).request)
        response = client.paginate("everything", params={"q": "python", "page": 1})
        # prints the raw HTTP response
        print(next(response).response)
        response = client.paginate("everything", params={"q": "python", "page": 1})
        # prints the paginator that was used
        print(next(response).paginator)
        response = client.paginate("everything", params={"q": "python", "page": 1})
        # prints the authentication class used
        return print(next(response).auth)


    _()
    return


@app.cell
def _(APIKeyAuth, PageNumberPaginator, RESTClient, api_key):
    # GET /v2/everything?q=python&page=1&pageSize=5&apiKey=...
    def _():
        client = RESTClient(
            base_url="https://newsapi.org/v2/",
            auth=APIKeyAuth(
                name="apiKey",
                api_key=api_key,
                location="query"
            ),
            paginator=PageNumberPaginator(
                base_page=1,                 # NewsAPI starts paging from 1
                page_param="page",           # Matches the API spec
                total_path=None,             # Set it to None explicitly
                stop_after_empty_page=True,  # Stop if no articles returned
                maximum_page=4               # Optional limit for dev/testing
            ),
        )

        for page in client.paginate("everything", params={"q": "python", "pageSize": 5, "language": "en"}):
            for article in page:
                print(article["title"])

    _()
    return


@app.cell
def _(APIKeyAuth, PageNumberPaginator, RESTClient, dlt):
    @dlt.resource(write_disposition="replace", name="python_articles")
    def get_articles(api_key: str = dlt.secrets.value):
        client = RESTClient(
            base_url="https://newsapi.org/v2/",
            auth=APIKeyAuth(
                name="apiKey",
                api_key=api_key,
                location="query"
            ),
            paginator=PageNumberPaginator(
                base_page=1,
                page_param="page",
                total_path=None,
                stop_after_empty_page=True,
                maximum_page=4
            ),
        )

        for page in client.paginate("everything", params={"q": "python", "pageSize": 5, "language": "en"}):
            yield page
    return (get_articles,)


@app.cell
def _(APIKeyAuth, PageNumberPaginator, RESTClient, dlt):
    @dlt.resource(write_disposition="replace", name="top_articles")
    def get_top_articles(api_key: str = dlt.secrets.value):
        client = RESTClient(
            base_url="https://newsapi.org/v2/",
            auth=APIKeyAuth(
                name="apiKey",
                api_key=api_key,
                location="query"
            ),
            paginator=PageNumberPaginator(
                base_page=1,
                page_param="page",
                total_path=None,
                stop_after_empty_page=True,
                maximum_page=4
            ),
        )

        for page in client.paginate("top-headlines", params={"pageSize": 5, "language": "en"}):
            yield page
    return (get_top_articles,)


@app.cell
def _(dlt, get_articles, get_top_articles):
    @dlt.source
    def newsapi_source(api_key: str = dlt.secrets.value):
        return [get_articles(api_key=api_key), get_top_articles(api_key=api_key)]
    return (newsapi_source,)


@app.cell
def _(dlt, newsapi_source):
    pipeline = dlt.pipeline(
        pipeline_name="newsapi_pipeline",
        destination="duckdb",
        dataset_name="news_data"
    )

    info = pipeline.run(newsapi_source())
    print(info)
    return info, pipeline


@app.cell
def _(pipeline):
    pipeline.dataset(dataset_type="default").python_articles.df().head()
    return


@app.cell
def _(pipeline):
    pipeline.dataset(dataset_type="default").top_articles.df().head()
    return


if __name__ == "__main__":
    app.run()
