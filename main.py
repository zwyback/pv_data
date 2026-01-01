import requests
import json
from datetime import datetime as dt
from time import sleep
from dataclasses import dataclass
import sqlite3
import pathlib

CONFIG_FILE = pathlib.Path(__file__).parent / "config.json"

with open(CONFIG_FILE) as f:
    CONFIG = json.load(f)

@dataclass
class Entry:
    timestamp: int = None
    P_PV: float = None
    P_Load: float = None
    P_Grid: float = None
    P_Akku: float = None
    SOC: float = None
    raw_json: str = None

    def __str__(self):
        return f" \
            Timestamp: {self.timestamp} \n \
            P_PV: {self.P_PV} \n \
            P_Load: {self.P_Load} \n \
            P_Grid: {self.P_Grid} \n \
            P_Akku: {self.P_Akku} \n \
            SOC: {self.SOC}\
        "




def init_database():
    """Erstellt die Datenbank-Tabelle falls nicht vorhanden"""
    conn = sqlite3.connect(CONFIG["db"]["filename"])
    cursor = conn.cursor()
    
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {CONFIG["db"]["table"]} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            P_PV REAL,
            P_Load REAL,
            P_Grid REAL,
            P_Akku REAL,
            SOC,
            raw_json TEXT
        )
    ''')
    
    conn.commit()
    conn.close()
    print("Datenbank initialisiert")

def fetch_data() -> Entry:
    url: str = CONFIG['api']['url']
    timestamp_field = CONFIG['api']["fields"]['timestamp_field']["field_name"]
    value_fields: list[str] = CONFIG["api"]["fields"]["value_fields"].keys()

    data = requests.get(url).json()
    return_entry: Entry = Entry()
    return_entry.raw_json = json.dumps(data)


    time_string: str = data[timestamp_field]['datestamp'] + " " + data[timestamp_field]['timestamp']
    time_format: str = "%d.%m.%Y %H:%M:%S"
    return_entry.timestamp = str(dt.strptime(time_string, time_format).timestamp())

    for field in value_fields:
        values = CONFIG["api"]["fields"]["value_fields"][field]
        for value in values:
            try:
                if field == "inverters":
                    setattr(return_entry, value, data[field][0][value])
                else:
                    setattr(return_entry, value, data[field][value])
            except Exception as e:
                print(f"Warning: Wert {value} in Feld {field} nicht enthalten")

    return return_entry

def add_db_entry(entry: Entry):
    conn = sqlite3.connect(CONFIG["db"]["filename"])
    cursor = conn.cursor()
    cursor.execute(f'''
        INSERT INTO {CONFIG["db"]["table"]} (timestamp, P_PV, P_Load, P_Grid, P_Akku, SOC, raw_json)
        VALUES ({entry.timestamp}, {entry.P_PV}, {entry.P_Load}, {entry.P_Grid}, {entry.P_Akku}, {entry.SOC}, '{entry.raw_json}')
    ''')
    conn.commit()
    conn.close()

if __name__ == "__main__":
    init_database()
    WAIT_TIME = CONFIG["api"]["wait_time_seconds"]
    while (1):
        entry = fetch_data()
        add_db_entry(entry)
        print(entry)
        print("------------")
        sleep(WAIT_TIME)