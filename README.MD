# Postcard Company Datamart

This project represents a data model build with `dbt-core` for an imaginary company selling postcards.

# Lore
The company sells both directly but also through resellers in the majority of European countries.

# Data model

## Layers

- `raw` unrefined input data
- `staging` staging area
- `core` curated data

## Dimensions
- dim_channel
- dim_customer
- dim_date
- dim_geography
- dim_sales_agent

## Facts
- fact_sales


# Getting started

The data is generated as parquet files by a Python script `generator/generate.py` using user-defined assets `assets.py`. These may be adjusted as per needs.


## Preparing the dbt project

1. Rename `.env.example` to `.env`. This will contain relative paths for the database file (datamart.duckdb) and parquet input files

2. Rename `shared\db\datamart.duckdb.example` to `shared\db\datamart.duckdb` or initiate an empty database there with the same name.


## Generating the data

1. Create a Python Virtual Environment

`python3 -m venv .venv`

2. Activate the Python venv

`source .venv/bin/activate`

3. Install the required packages

`pip install -r generator/requirements.txt`

4. Generate the data

`python generator/generate.py`

The generated data will be under `shared/parquet`.


## Running the dbt model

1. Ensure the virtual environment is activated

`source .venv/bin/activate`

2. Change directory to `postcard_company`

`cd postcard_company`

3. Run `dbt deps` to install dependencies

4. Run `dbt seed` to import the seed data

5. Run `dbt compile` to check everything is in order

6. Run `dbt run` to run the models

7. Run `dbt test` to run the tests


