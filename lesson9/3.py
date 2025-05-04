import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

client = RESTClient(
        base_url="https://jaffle-shop.scalevector.ai/api/v1",
        paginator=PageNumberPaginator(
                base_page=1,
                page_param="page",
                stop_after_empty_page=True,
                total_path=None,                # API does not have total field
                # maximum_page=4,               # Optional limit for dev/testing
            ),
    )

@dlt.resource(write_disposition="replace")
def customers():
    for page in client.paginate("/customers", params={"size": 1000}):
        yield page

@dlt.resource(write_disposition="replace", parallelized=True)
def orders():
    for page in client.paginate("/orders", params={"size": 1000}):
        yield page

@dlt.resource(write_disposition="replace")
def products():
    for page in client.paginate("/products"):
        yield page

@dlt.source
def jaffle_source():
    return [orders]
    return [customers, orders, products]    

pipeline = dlt.pipeline(
    pipeline_name="jaffle_pipeline",
    destination="duckdb",
    dataset_name="jaffle"
)

info = pipeline.run(jaffle_source())
print(info)
print(pipeline.last_trace)
with pipeline.sql_client() as sql_client:
    with sql_client.execute_query("SELECT COUNT(*) AS total_rows, 'customers' AS table FROM customers UNION ALL SELECT COUNT(*), 'orders' FROM orders UNION ALL SELECT COUNT(*), 'products' FROM products") as table:
        print(table.df())

#    total_rows      table
# 0         935  customers
# 1       61948     orders
# 2          10   products

# 0.06s