

import marimo

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import dlt
    from dlt.sources.filesystem import filesystem, read_parquet
    import os
    from dlt.sources.filesystem import filesystem
    from dlt.common.storages.fsspec_filesystem import FileItemDict
    return FileItemDict, dlt, filesystem, os, read_parquet


@app.cell
def _(dlt, filesystem, read_parquet):
    def _():
        # Point to the local file directory
        fs = filesystem(bucket_url="./local_data", file_glob="**/*.parquet")
    
        # Add a transformer
        parquet_data = fs | read_parquet()
    
        # Create and run pipeline
        pipeline = dlt.pipeline(pipeline_name="my_pipeline", destination="duckdb")
        load_info = pipeline.run(parquet_data.with_name("userdata"))
        print(load_info)
    
        # Inspect data
        pipeline.dataset().userdata.df().head()

        df = pipeline.dataset().userdata.df()
        df.groupby("gender").describe()
    return


@app.cell
def _(dlt, filesystem):
    @dlt.transformer()
    def read_parquet_with_filename(files):
        import pyarrow.parquet as pq
        for file_item in files:
            with file_item.open() as f:
                table = pq.read_table(f).to_pandas()
                table["source_file"] = file_item["file_name"]
                yield table.to_dict(orient="records")

    def _():
        fs = filesystem(bucket_url="./local_data", file_glob="*.parquet")
        pipeline = dlt.pipeline("meta_pipeline", destination="duckdb")
    
        load_info = pipeline.run((fs | read_parquet_with_filename()).with_name("userdata"))
        print(load_info)
    return


@app.cell
def _(dlt, filesystem, read_parquet):
    fs = filesystem(bucket_url="./local_data", file_glob="**/*.parquet")

    # Only include files that contain "user" and are < 1MB
    fs.add_filter(lambda f: "user" in f["file_name"] and f["size_in_bytes"] < 1_000_000)

    pipeline = dlt.pipeline("filtered_pipeline", destination="duckdb")
    load_info = pipeline.run((fs | read_parquet()).with_name("userdata_filtered"))
    print(load_info)
    return


@app.cell
def _(dlt, filesystem, read_parquet):
    def _():
        fs = filesystem(bucket_url="./local_data", file_glob="**/*.parquet")
        fs.apply_hints(incremental=dlt.sources.incremental("modification_date"))

        data = (fs | read_parquet()).with_name("userdata")
        pipeline = dlt.pipeline("incremental_pipeline", destination="duckdb")
        load_info = pipeline.run(data)
        return print(load_info)


    _()
    return


@app.cell
def _(dlt, filesystem):
    def _():
        @dlt.transformer(standalone=True)
        def read_json(items):
            from dlt.common import json
            for file_obj in items:
                with file_obj.open() as f:
                    yield json.load(f)

        fs = filesystem(bucket_url="./local_data", file_glob="sample.json")
        pipeline = dlt.pipeline("json_pipeline", destination="duckdb")

        load_info = pipeline.run((fs | read_json()).with_name("users"))
        print(load_info)
        return pipeline.dataset().users.df().head()


    _()
    return


@app.cell
def _(FileItemDict, dlt, filesystem, os):
    def _():
        def copy_local(item: FileItemDict) -> FileItemDict:
            local_path = os.path.join("copied", item["file_name"])
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            item.fsspec.download(item["file_url"], local_path)
            return item

        fs = filesystem(bucket_url="./local_data", file_glob="**/*.parquet").add_map(copy_local)
        pipeline = dlt.pipeline("copy_pipeline", destination="duckdb")
        load_info = pipeline.run(fs.with_name("copied_files"))
        return print(load_info)


    _()
    return


if __name__ == "__main__":
    app.run()
