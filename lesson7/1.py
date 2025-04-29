

import marimo

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import dlt
    import duckdb
    return dlt, duckdb


@app.cell
def _():
    # Sample data to be loaded
    data = [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"}]

    return (data,)


@app.cell
def _(dlt):
    # Create a dlt pipeline
    table_pipeline = dlt.pipeline(
        pipeline_name="data_contracts_table_level", destination="duckdb", dataset_name="mydata"
    )
    return (table_pipeline,)


@app.cell
def _(data, table_pipeline):
    def _():
        # Load the data to the "users" table
        load_info = table_pipeline.run(data, table_name="users")
        print(load_info)
    
        # Print the row counts for each table that was loaded in the last run of the pipeline
        print("\nNumber of new rows loaded into each table: ", table_pipeline.last_trace.last_normalize_info.row_counts)

    _()
    return


@app.cell
def _(data, dlt, table_pipeline):
    def _():
        # Define a dlt resource that allows the creation of new tables
        @dlt.resource(schema_contract={"tables": "evolve"})
        def allow_new_tables(input_data):
          yield input_data
    
        # Run the pipeline again with the above dtl resource to load the same data into a new table "new_users"
        load_info = table_pipeline.run(allow_new_tables(data), table_name="new_users")
        print(load_info)
    
        # Print the row counts for each table that was loaded in the last run of the pipeline
        print("\nNumber of new rows loaded into each table: ", table_pipeline.last_trace.last_normalize_info.row_counts)

    _()
    return


@app.cell
def _(data, dlt, table_pipeline):
    def _():
        # Define a dlt resource that prevents any changes to the schema at the table level (no new tables can be added)
        @dlt.resource(schema_contract={"tables": "freeze"})
        def no_new_tables(input_data):
          yield input_data
    
        # Now, run the pipeline with the resource above, attempting to load the same data into "newest_users".
        # This will fail, as new tables can't be added.
        load_info = table_pipeline.run(no_new_tables(data), table_name="newest_users")
        print(load_info)

    _()
    return


@app.cell
def _(dlt):
    # Create a new pipeline
    column_pipeline = dlt.pipeline(
        pipeline_name="data_contracts_column_level", destination="duckdb", dataset_name="mydata"
    )
    return (column_pipeline,)


@app.cell
def _(column_pipeline):
    def _():
        # Load the initial data containing columns "id" and "name" into the "users" table
        load_info = column_pipeline.run([{"id": 1, "name": "Alice"}], table_name="users")
        print(load_info)

    _()
    return


@app.cell
def _(column_pipeline, duckdb):
    conn = duckdb.connect(f"{column_pipeline.pipeline_name}.duckdb")
    return (conn,)


@app.cell
def _(conn):
    conn.sql("SELECT * FROM mydata.users").df()
    return


@app.cell
def _(column_pipeline, conn, dlt):
    def _():

        # Define dlt resource that allows new columns in the data
        @dlt.resource(schema_contract={"columns": "evolve"})
        def allow_new_columns(input_data):
            yield input_data
    
        # Now, load a new row into the same table, "users", which includes an additional column "age"
        load_info = column_pipeline.run(allow_new_columns([{"id": 2, "name": "Bob", "age": 35}]), table_name="users")
        print(load_info)
        print(conn.sql("SELECT * FROM mydata.users").df())

    _()
    return


@app.cell
def _(column_pipeline, conn, dlt):
    def _():
        # Define a dlt resource that skips rows that have new columns but loads those that follow the existing schema
        @dlt.resource(schema_contract={"columns": "discard_row"})
        def discard_row(input_data):
           yield input_data
    
        # Attempt to load two additional rows. Only the row that follows the existing schema will be loaded
        load_info = column_pipeline.run(
            discard_row([
                {"id": 3, "name": "Sam", "age": 30}, # This row will be loaded
                {"id": 4, "name": "Kate", "age": 79, "phone": "123-456-7890"} # This row will not be loaded
            ]),
            table_name="users"
        )
        print(load_info)
        print(conn.sql("SELECT * FROM mydata.users").df())

    _()
    return


@app.cell
def _(column_pipeline, conn, dlt):
    def _():
        # Define a dlt resource that only skips the values of new columns, loading the rest of the row data
        @dlt.resource(schema_contract={"columns": "discard_value"})
        def discard_value(input_data):
           yield input_data
    
        # Load two additional rows. Since we're using the "discard_value" resource, both rows will be added
        # However, the "phone" column in the second row will be ignored and not loaded
        load_info = column_pipeline.run(
            discard_value([
                {"id": 5, "name": "Sarah", "age": "23"},
                {"id": 6, "name": "Violetta", "age": "22", "phone": "666-513-4510"}
            ]),
            table_name="users"
        )
        print(load_info)
        print(conn.sql("SELECT * FROM mydata.users").df())

    _()
    return


@app.cell
def _(column_pipeline, dlt):
    def _():
        # Define a dlt resource that does not allow new columns in the data
        @dlt.resource(schema_contract={"columns": "freeze"})
        def no_new_columns(input_data):
          yield input_data
    
        # Attempt to load a row with additional columns when the column contract is set to freeze
        # This will fail as no new columns are allowed.
        load_info = column_pipeline.run(
            no_new_columns([
                {"id": 7, "name": "Lisa", "age": 40, "phone": "098-765-4321"}
            ]),
            table_name="users"
        )
        print(load_info)

    _()
    return


@app.cell
def _(dlt, duckdb):
    # Create a pipeline for loading data
    data_type_pipeline = dlt.pipeline(
        pipeline_name="data_contracts_data_type", destination="duckdb", dataset_name="mydata"
    )
    conn2 = duckdb.connect(f"{data_type_pipeline.pipeline_name}.duckdb")
    return conn2, data_type_pipeline


@app.cell
def _(conn2, data_type_pipeline):
    def _():
        # Load the initial data containing a column "age" of type int
        load_info = data_type_pipeline.run([{"id": 1, "name": "Alice", "age": 24}], table_name="users")
        print(load_info)
        print(conn2.sql("SELECT * FROM mydata.users").df())

    _()
    return


@app.cell
def _(conn2, data_type_pipeline, dlt):
    def _():
        # Define dlt resource that accepts all data types
        @dlt.resource(schema_contract={"data_type": "evolve"})
        def allow_any_data_type(input_data):
            yield input_data
    
        # Now, load a new row where the "age" column is passed as a string but will be validated and stored as an integer
        load_info = data_type_pipeline.run(allow_any_data_type([{"id": 2, "name": "Bob", "age": "35"}]), table_name="users")
        print(load_info)
        print(conn2.sql("SELECT * FROM mydata.users").df())

    _()
    return


@app.cell
def _(conn2, dlt, load_info):
    def _():
        # Define dlt resource that accepts all data types
        @dlt.resource(schema_contract={"data_type": "evolve"})
        def allow_any_data_type(input_data):
            yield input_data
    
        # If you pass the age as "thirty-five", a new variant column will be added
        # Note: Running the uncommented code below may affect subsequent steps, so proceed with caution
        #load_info = data_type_pipeline.run(allow_any_data_type([{"id": 2, "name": "Bob", "age": "thirty-five"}]), table_name="users")
        #print(load_info)
        print(load_info)
        print(conn2.sql("SELECT * FROM mydata.users").df())

    _()
    return


@app.cell
def _(conn2, data_type_pipeline, dlt):
    def _():
        # Define dlt resource that omits rows with unverifiable data types
        @dlt.resource(schema_contract={"data_type": "discard_row"})
        def discard_row(input_data):
           yield input_data
    
        # Attempt to load two additional rows. Only the row where all column types can be validated will be loaded
        load_info = data_type_pipeline.run(
            discard_row([
                {"id": 3, "name": "Sam", "age": "35"}, # This row will be loaded
                {"id": 4, "name": "Kate", "age": "seventy"} # This row will not be loaded
            ]),
            table_name="users"
        )
        print(load_info)
        print(conn2.sql("SELECT * FROM mydata.users").df())

    _()
    return


@app.cell
def _(conn2, data_type_pipeline, dlt):
    def _():
        # Define a dlt resource that replaces unverifiable values with None, but retains the rest of the row data
        @dlt.resource(schema_contract={"data_type": "discard_value"})
        def discard_value(input_data):
           yield input_data
    
        # Load two additional rows. Since we're using the "discard_value" resource, both rows will be added
        # However, the "age" value "twenty-eight" in the second row will be ignored and not loaded
        load_info = data_type_pipeline.run(
            discard_value([
                {"id": 5, "name": "Sarah", "age": 23},
                {"id": 6, "name": "Violetta", "age": "twenty-eight"}
            ]),
            table_name="users"
        )
        print(load_info)
        print(conn2.sql("SELECT * FROM mydata.users").df())

    _()
    return


@app.cell
def _(data_type_pipeline, dlt):
    def _():
        # Define dlt resource that prevents any changes to the existing data types
        @dlt.resource(schema_contract={"data_type": "freeze"})
        def no_data_type_changes(input_data):
          yield input_data
    
        # Attempt to load a row with a column value that can't be validated, in this case "forty"
        # This will fail as no data type changes are allowed with the "no_data_type_changes" resource
        load_info = data_type_pipeline.run(no_data_type_changes([{"id": 7, "name": "Lisa", "age": "forty"}]), table_name="users")
        print(load_info)

    _()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
