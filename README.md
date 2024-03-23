# Klondike

<img src="https://upload.wikimedia.org/wikipedia/en/d/d5/Klondike_logo.svg">

Klondike offers a lightweight API to read and write data to Google BigQuery using rust-optimized Polars DataFrames.

## Installation

### Installing Klondike
Install at the command line

```
pip install klondike==0.1.0
```

### Installing Rust
Since Polars leverages Rust speedups, you need to have Rust installed in your environment as well. See the Rust installation guide [here](https://www.rust-lang.org/tools/install).


## Usage

```
# Instantiate a connection to BigQuery
from klondike import BigQueryConnector

bq = BigQueryConnector(
    app_creds="/path/to/your/service_account.json"
)

# Read data from BigQuery
sql = "SELECT * FROM nba_dbt.staging__nyk_players"
df = bq.read_dataframe_from_bigquery(sql=sql)

# Write data to BigQuery
bq.write_dataframe_to_bigquery(
    df=df,
    table_name="nba_dbt.my_new_table",
    if_eixsts="truncate"
)
```