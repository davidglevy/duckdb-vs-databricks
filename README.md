This is an attempt to convert the comparison benchmark between DuckDB and Apache Spark on the TCP-H benchmark found in this repo
to work with Databricks

https://github.com/djouallah/Testing_BI_Engine

## Goals
Try compare apples to apples against these two technologies:
1.) Moving DuckDB to instance storage is not painless and should be part of the total time
2.) Transforming datasets (Parquet to either Delta or DuckDB Native should also be part of the comparison)
3.) Other hidden costs, like preserving the DuckDB Native database and rehydrating it should also be highlighted

## Setup
Create a single node Photon cluster

## Running the Tests
The original benchmark ignores a few parts of the time taken to run:
1.) Downloading the data to the driver
2.) Transforming from parquet into Native DuckDB

