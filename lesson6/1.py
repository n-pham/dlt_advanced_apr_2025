

import marimo

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import dlt
    return (dlt,)


@app.cell
def _(dlt):
    data = [
        {"id": "1", "name": "bulbasaur", "size": {"weight": 6.9, "height": 0.7}},
        {"id": "4", "name": "charmander", "size": {"weight": 8.5, "height": 0.6}},
        {"id": "25", "name": "pikachu", "size": {"weight": 6, "height": 0.4}},
    ]


    dlt.secrets["destination.replace_strategy"] = "truncate-and-insert" # <--- set the replace strategy using TOML, ENVs or Python

    return (data,)


@app.cell
def _(data, dlt):
    def _():
        pipeline = dlt.pipeline(
            pipeline_name="pokemon_load_1",
            destination="duckdb",
            dataset_name="pokemon_data_1",
        )
    
        load_info = pipeline.run(data, table_name="pokemon", write_disposition="replace")
        print(pipeline.last_trace)
    
        with pipeline.sql_client() as client:
            with client.execute_query("SHOW ALL TABLES") as table:
                tables = table.df()
                print(tables)

    _()
    return


@app.cell
def _(data, dlt):
    def _():
        dlt.secrets["destination.replace_strategy"] = "insert-from-staging"
    
        pipeline = dlt.pipeline(
            pipeline_name="pokemon_load_2",
            destination="duckdb",
            dataset_name="pokemon_data_2",
        )
    
        load_info = pipeline.run(data, table_name="pokemon", write_disposition="replace")
        print(pipeline.last_trace)
    
        with pipeline.sql_client() as client:
            with client.execute_query("SHOW ALL TABLES") as table:
                tables = table.df()
                print(tables)

    _()
    return


@app.cell
def _(dlt):
    def _():
        pipeline = dlt.pipeline(
            pipeline_name="pokemon_load_2",
            destination="duckdb",
            dataset_name="pokemon_data_2",
        )
        with pipeline.sql_client() as client:
            with client.execute_query("SELECT * from pokemon_data_2_staging.pokemon") as table:
                tables = table.df()
                print(tables)

    _()
    return


@app.cell
def _(data, dlt):
    @dlt.resource(
        name='pokemon',
        write_disposition='merge',
        primary_key="id",
    )
    def pokemon():
        yield data


    pipeline = dlt.pipeline(
        pipeline_name="poke_pipeline_merge",
        destination="duckdb",
        dataset_name="pokemon_data",
    )

    load_info = pipeline.run(pokemon)
    print(load_info)

    # explore loaded data
    pipeline.dataset(dataset_type="default").pokemon.df()


    return


@app.cell
def _(data, dlt):
    def _():
        @dlt.resource(
            name='pokemon',
            write_disposition={
                "disposition": "merge", # <--- specifies that existing data should be merged
                "strategy": "scd2" # <--- enables SCD2 tracking, which keeps historical records of changes
            },
            primary_key="id",
        )
        def pokemon():
            yield data

        pipeline = dlt.pipeline(
            pipeline_name="pokemon_pipeline",
            destination="duckdb",
            dataset_name="pokemon_scd2",
        )


        load_info = pipeline.run(pokemon)
        print(load_info)

        print(pipeline.dataset(dataset_type="default").pokemon.df())


    _()
    return


@app.cell
def _(dlt):
    def _():
        data = [
            {"id": "1", "name": "bulbasaur", "size": {"weight": 6.9, "height": 0.7}, "deleted_flag": True},  # <--- should be deleted
            {"id": "4", "name": "charmander", "size": {"weight": 8.5, "height": 0.6}, "deleted_flag": None},  # <--- should be kept
            {"id": "25", "name": "pikachu", "size": {"weight": 6, "height": 0.4}, "deleted_flag": False},  # <--- should be kept
        ]

        @dlt.resource(
            name='pokemon',
            write_disposition='merge',
            primary_key="id",
            columns={"deleted_flag": {"hard_delete": True}} # <--- set columns argument
        )
        def pokemon():
            yield data

        pipeline = dlt.pipeline(
            pipeline_name="pokemon_pipeline",
            destination="duckdb",
            dataset_name="pokemon_hd",
        )


        load_info = pipeline.run(pokemon)
        return print(load_info)


    _()
    return


@app.cell
def _():
    # TODO Missing incremental cursor path
    # TODO Backfilling
    return


if __name__ == "__main__":
    app.run()
