import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
import os

client = RESTClient(
        base_url="https://jaffle-shop.scalevector.ai/api/v1",
        paginator=PageNumberPaginator(
                base_page=1,
                page_param="page",
                stop_after_empty_page=True,
                total_path=None,                # API does not have total field
                # maximum_page=7               # Optional limit for dev/testing
            ),
    )

# Result
#    total_rows      table
# 0         935  customers
# 1       61948     orders
# 2          10   products

# page_size 5K+ will time out
# Step extract COMPLETED in 13 minutes                  default
# Step extract COMPLETED in 9 minutes and 10.66 seconds page_size 3900 workers 20 FILE_MAX_ITEMS 3900
# Step extract COMPLETED in 8 minutes and 36.19 seconds page_size 500 workers 20 FILE_MAX_ITEMS 1000
# Step extract COMPLETED in 8 minutes and 17.81 seconds page_size 500 workers 20 FILE_MAX_ITEMS 5000
# Step extract COMPLETED in 7 minutes and 37.83 seconds page_size 900 workers 20 FILE_MAX_ITEMS 9000
# Step extract COMPLETED in 7 minutes and 23.03 seconds page_size 2900 workers 20 FILE_MAX_ITEMS 2900
# Step extract COMPLETED in 7 minutes and 9.16 seconds page_size 1900 workers 10 FILE_MAX_ITEMS 1900
# Step extract COMPLETED in 6 minutes and 45.86 seconds page_size 1900 workers 20 FILE_MAX_ITEMS 19000
# Step extract COMPLETED in 6 minutes and 43.13 seconds page_size 1900 workers 30 FILE_MAX_ITEMS 1900
# Step extract COMPLETED in 6 minutes and 28.87 seconds page_size 1900 workers 20 FILE_MAX_ITEMS 1900
# Step extract COMPLETED in 6 minutes and 9.75 seconds page_size 2000 workers 16 FILE_MAX_ITEMS 2000

# extract took most time --> tune extract mostly
# Example
# Step extract COMPLETED in 8 minutes and 36.19 seconds
# Step normalize COMPLETED in 3.27 seconds
# Step load COMPLETED in 1.99 seconds
# Step run COMPLETED in 8 minutes and 41.47 seconds

PAGE_SIZE = 2000 # 5000 504 time out
os.environ["EXTRACT__WORKERS"] = "16"
os.environ["DATA_WRITER__FILE_MAX_ITEMS"]="2000"
os.environ["DATA_WRITER__FILE_MAX_BYTES"]="100000"
# os.environ["NORMALIZE__WORKERS"] = "2"
# os.environ["LOAD__WORKERS"] = "2"

@dlt.resource(write_disposition="replace")
def customers():
    for page in client.paginate("/customers", params={
        "page_size": PAGE_SIZE
        }):
        yield page

@dlt.resource(write_disposition="replace", parallelized=True)
def orders():
    for page in client.paginate("/orders", params={
        "page_size": PAGE_SIZE 
        }):
        yield page

@dlt.resource(write_disposition="replace")
def products():
    for page in client.paginate("/products"):
        yield page

@dlt.source
def jaffle_source():
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




