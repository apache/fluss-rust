import asyncio
import os
from time import sleep

import lance
import pandas as pd
import pyarrow as pa
from tqdm import tqdm

import fluss_python as fluss

table_name = "example"


def create_connection(config_spec):
    # Create connection using the static connect method
    async def _inner():
        config = fluss.Config(config_spec)
        conn = await fluss.FlussConnection.connect(config)
        return conn

    return asyncio.run(_inner())


def create_table(conn, table_path, pa_schema):
    # Create a Fluss TableDescriptor
    fluss_schema = fluss.Schema(pa_schema)
    # Create a Fluss TableDescriptor
    table_descriptor = fluss.TableDescriptor(
        fluss_schema,
        properties={
            "table.datalake.enabled": "true",
            "table.datalake.freshness": "30s",
        },
    )

    async def _inner():
        # Get the admin for Fluss
        admin = await conn.get_admin()

        # Create a Fluss table
        try:
            await admin.create_table(table_path, table_descriptor, True)
            print(f"Created table: {table_path}")
        except Exception as e:
            print(f"Table creation failed: {e}")

    asyncio.run(_inner())


def write_to_fluss(conn, table_path, pa_schema):
    # Get table and create writer‘
    async def _inner():
        table = await conn.get_table(table_path)
        append_writer = await table.new_append_writer()
        try:
            print("\n--- Writing image data ---")
            for record_batch in process_images(pa_schema):
                append_writer.write_arrow_batch(record_batch)
        except Exception as e:
            print(f"Failed to write image data: {e}")
        finally:
            append_writer.close()

    asyncio.run(_inner())


def process_images(schema: pa.Schema):
    # Get the current directory path
    current_dir = os.getcwd()
    images_folder = os.path.join(current_dir, "image")

    # Get the list of image files
    image_files = [
        filename
        for filename in os.listdir(images_folder)
        if filename.endswith((".png", ".jpg", ".jpeg"))
    ]

    # Iterate over all images in the folder with tqdm
    for filename in tqdm(image_files, desc="Processing Images"):
        # Construct the full path to the image
        image_path = os.path.join(images_folder, filename)

        # Read and convert the image to a binary format
        with open(image_path, "rb") as f:
            binary_data = f.read()

        image_array = pa.array([binary_data], type=pa.binary())

        # Yield RecordBatch for each image
        yield pa.RecordBatch.from_arrays([image_array], schema=schema)


def loading_into_pandas():
    uri = "s3://lance/fluss/" + table_name + ".lance"
    ds = lance.dataset(
        uri,
        storage_options={
            "access_key_id": "minio",
            "secret_access_key": "minioadmin",
            "endpoint": "http://localhost:9000",
            "allow_http": "true",
        },
    )

    # Accumulate data from batches into a list
    data = []
    for batch in ds.to_batches(columns=["image"], batch_size=10):
        tbl = batch.to_pandas()
        data.append(tbl)

    # Concatenate all DataFrames into a single DataFrame
    df = pd.concat(data, ignore_index=True)
    print("Pandas DataFrame is ready")
    print("Total Rows: ", df.shape[0])
    return df


if __name__ == "__main__":
    config_spec = {
        "bootstrap.servers": "127.0.0.1:9123",
    }
    conn = create_connection(config_spec)
    table_path = fluss.TablePath("fluss", table_name)
    pa_schema = pa.schema([("image", pa.binary())])
    create_table(conn, table_path, pa_schema)
    write_to_fluss(conn, table_path, pa_schema)
    sleep(60)
    df = loading_into_pandas()
    print(df.head())
    conn.close()
