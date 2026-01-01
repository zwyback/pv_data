import pandas as pd
import sqlite3
import pathlib
import json

CONFIG_FILE = pathlib.Path(__file__).parents[1] / "config" / "config.json"

with open(CONFIG_FILE) as f:
    CONFIG = json.load(f)

DB_FILE = pathlib.Path(__file__).parents[1] / "data" / CONFIG["db"]["filename"]
CSV_FILE = pathlib.Path(__file__).parents[1] / "data" / CONFIG["export"]["filename"]


connection = sqlite3.connect(DB_FILE)
df = pd.read_sql_query(f"SELECT * FROM {CONFIG['db']['table']}", connection)
df.to_csv(CSV_FILE, index=False, sep=";", decimal=".")

connection.close()