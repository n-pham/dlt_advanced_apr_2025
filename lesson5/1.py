

import marimo

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import dlt
    from dlt.sources.sql_database import sql_database
    import sqlalchemy as sa
    import hashlib

    import pyarrow as pa
    return dlt, hashlib, pa, sa, sql_database


@app.cell
def _(dlt, sql_database):
    def _():
        source = sql_database(
            "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam",
            table_names=["family","genome"]
        )
    
        pipeline = dlt.pipeline(
            pipeline_name="sql_database_pipeline",
            destination="duckdb",
            dataset_name="sql_data",
        )
        load_info = pipeline.run(source)
        print(load_info)

    _()
    return


@app.cell
def _(pipeline):
    with pipeline.sql_client() as client:
      with client.execute_query("SELECT * FROM genome") as table:
          genome = table.df()
    genome
    return


@app.cell
def _(pipeline):
    def _():
        with pipeline.sql_client() as client:
          with client.execute_query("SELECT COUNT(*) AS total_rows FROM genome") as table:
            return print(table.df())


    _()
    return


@app.cell
def _(pipeline):
    def _():
        with pipeline.sql_client() as client:
          with client.execute_query("SELECT COUNT(*) AS total_rows FROM genome WHERE kingdom='bacteria'") as table:
            return print(table.df())


    _()
    return


@app.function
def query_adapter_callback(query, table):
    if table.name == "genome":
        # Only select rows where the column kingdom has value "bacteria"
        return query.where(table.c.kingdom=="bacteria")
    # Use the original query for other tables
    return query


@app.cell
def _(dlt, sql_database):
    source = sql_database(
        "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam",
        table_names=["genome"],
        query_adapter_callback=query_adapter_callback
    )


    pipeline = dlt.pipeline(
        pipeline_name="sql_database_pipeline_filtered",
        destination="duckdb",
        dataset_name="sql_data"
    )

    load_info = pipeline.run(source, write_disposition="replace")

    print(pipeline.last_trace)
    return (pipeline,)


@app.cell
def _(sa):
    def query_adapter_callback_2(query, table):
        if table.name == "genome":
            # Only select rows where the column kingdom has value "bacteria"
            return sa.text(f"SELECT * FROM {table.fullname} WHERE kingdom='bacteria'")
        return query
    return (query_adapter_callback_2,)


@app.cell
def _(dlt, query_adapter_callback_2, sql_database):
    def _():
        source = sql_database(
            "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam",
            table_names=["genome", "clan"],
            query_adapter_callback=query_adapter_callback_2
        )
    
    
        pipeline = dlt.pipeline(
            pipeline_name="sql_database_pipeline_filtered",
            destination="duckdb",
            dataset_name="sql_data"
    
        )
    
        load_info = pipeline.run(source, write_disposition="replace")
    
        print(load_info)

    _()
    return


@app.cell
def _(pipeline):
    def _():
        with pipeline.sql_client() as client:

            with client.execute_query("SELECT COUNT(*) AS total_rows, MAX(_dlt_load_id) as latest_load_id FROM clan") as table:
                print("Table clan:")
                print(table.df())
                print("\n")

            with client.execute_query("SELECT COUNT(*) AS total_rows, MAX(_dlt_load_id) as latest_load_id FROM genome") as table:
                print("Table genome:")
                print(table.df())


    _()
    return


@app.cell
def _(pipeline):
    def _():
        with pipeline.sql_client() as client:

            with client.execute_query("SELECT DISTINCT author FROM clan LIMIT 5") as table:
                print("Table clan:")
                print(table.df())


    _()
    return


@app.cell
def _(hashlib):
    def pseudonymize_name(row):
        '''
        Pseudonymization is a deterministic type of PII-obscuring.
        Its role is to allow identifying users by their hash,
        without revealing the underlying info.
        '''
        # add a constant salt to generate
        salt = 'WI@N57%zZrmk#88c'
        salted_string = row['author'] + salt
        sh = hashlib.sha256()
        sh.update(salted_string.encode())
        hashed_string = sh.digest().hex()
        row['author'] = hashed_string
        return row
    return (pseudonymize_name,)


@app.cell
def _(dlt, pseudonymize_name, sql_database):
    def _():
        pipeline = dlt.pipeline(
            pipeline_name="sql_database_pipeline_anonymized",
            destination="duckdb",
            dataset_name="sql_data"
        )


        source = sql_database("mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam", table_names=["clan"])

        source.clan.add_map(pseudonymize_name) # Apply the anonymization function to the extracted data

        info = pipeline.run(source)
        print(info)

        with pipeline.sql_client() as client:

            with client.execute_query("SELECT DISTINCT author FROM clan LIMIT 5") as table:
                print("Table clan:")
                clan = table.df()
        print(clan)
    _()
    return


@app.cell
def _(hashlib, pa):
    def pseudonymize_name_pyarrow(table: pa.Table) -> pa.Table:
        """
        Pseudonymizes the 'author' column in a PyArrow Table.
        """
        salt = 'WI@N57%zZrmk#88c'

        # Convert PyArrow Table to Pandas DataFrame for hashing
        df = table.to_pandas()

        # Apply SHA-256 hashing
        df['author'] = df['author'].astype(str).apply(lambda x: hashlib.sha256((x + salt).encode()).hexdigest())

        # Convert back to PyArrow Table
        new_table = pa.Table.from_pandas(df)

        return new_table
    return (pseudonymize_name_pyarrow,)


@app.cell
def _(dlt, pseudonymize_name_pyarrow, sql_database):
    def _():
        pipeline = dlt.pipeline(
            pipeline_name="sql_database_pipeline_anonymized1",
            destination="duckdb",
            dataset_name="sql_data"
        )
    
    
        source = sql_database(
            "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam",
            table_names=["clan"],
            backend="pyarrow",
            )
    
        source.clan.add_map(pseudonymize_name_pyarrow) # Apply the anonymization function to the extracted data
    
        info = pipeline.run(source)
        print(info)
    
        with pipeline.sql_client() as client:
    
            with client.execute_query("SELECT DISTINCT author FROM clan LIMIT 5") as table:
                print("Table clan:")
                print(table.df())

    _()
    return


@app.cell
def _(dlt, sql_database):
    def _():
        source = sql_database(
            "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam",
            table_names=["genome"]
        )
    
    
        pipeline = dlt.pipeline(
            pipeline_name="sql_database_pipeline_filtered",
            destination="duckdb",
            dataset_name="sql_data"
        )
        source.genome.add_filter(lambda item: item["kingdom"] == "bacteria")
    
        load_info = pipeline.run(source, write_disposition="replace")
    
        print(pipeline.last_trace)

        with pipeline.sql_client() as client:
            with client.execute_query("SELECT COUNT(*) AS total_rows, MAX(_dlt_load_id) as latest_load_id FROM genome") as table:
                print("Table genome:")
                genome_count = table.df()
                print(genome_count)

    _()
    return


@app.cell
def _(dlt, sql_database):
    def _():
        source = sql_database(
            "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam",
            table_names=["genome"],
            chunk_size=10,
        )
    
    
        pipeline = dlt.pipeline(
            pipeline_name="sql_database_pipeline_filtered",
            destination="duckdb",
            dataset_name="sql_data"
        )
        source.genome.add_limit(1)
    
        load_info = pipeline.run(source, write_disposition="replace")
    
        print(pipeline.last_trace)
    
        with pipeline.sql_client() as client:
          with client.execute_query("SELECT * FROM genome") as table:
              genome_limited = table.df()
              print(genome_limited)

    _()
    return


@app.cell
def _(dlt):
    @dlt.transformer()
    def batch_stats(items):
        '''
        Pseudonymization is a deterministic type of PII-obscuring.
        Its role is to allow identifying users by their hash,
        without revealing the underlying info.
        '''
        # add a constant salt to generate
        yield {"batch_length": len(items), "max_length": max([item["total_length"] for item in items])}


    return (batch_stats,)


@app.cell
def _(batch_stats, dlt, sql_database):
    def _():

        source = sql_database(
            "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam",
            chunk_size=10000
            ).genome


        pipeline = dlt.pipeline(
            pipeline_name="sql_database_pipeline_with_transformers1",
            destination="duckdb",
            dataset_name="sql_data",
            dev_mode=True
        )

        info = pipeline.run([source, source | batch_stats])
        print(pipeline.last_trace)

        with pipeline.sql_client() as client:
            with client.execute_query("SELECT * FROM batch_stats") as table:
                res = table.df()
                print(res)
            
    _()
    return


@app.cell
def _(pipeline):
    dataset = pipeline.dataset()

    # List tables
    dataset.row_counts().df()
    return (dataset,)


@app.cell
def _(dataset):
    # Access as pandas
    df = dataset["genome"].df()
    df
    return


@app.cell
def _(dataset):
    # Access as Arrow
    arrow_table = dataset["genome_length"].arrow()
    arrow_table
    return


@app.cell
def _(dataset):
    df2 = dataset["genome"].select("kingdom", "ncbi_id").limit(10).df()
    df2
    return


@app.cell
def _(dataset):
    for chunk in dataset["genome"].iter_df(chunk_size=500):
        print(chunk.head())
    return


if __name__ == "__main__":
    app.run()
