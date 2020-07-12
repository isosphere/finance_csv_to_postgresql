# finance_csv_to_postgresql
Inserts market data into a PostgreSQL database from CSV-formatted files.

Designed for the insertion of market data exported from MultiCharts QuoteManager.
Expects a QuoteManager-standard naming convention: SYMBOL-DATASOURCE-EXCHANGE-TYPE-TIMEFRAME-FIELD.csv.
SYMBOLs are deconstructed if they appear to be Futures, and will be converted to the base symbol with a new CONTRACT field added.
Will create its own table, 'bars', if executed with --create.
