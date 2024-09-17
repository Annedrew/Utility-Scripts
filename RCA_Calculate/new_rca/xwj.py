"""
xwj:  Total export value of all commodities from a country to country j.

selected country: 'China', 'Norway', 'Denmark', 'India', 'Italy'

output file columns: year, exporter, importer, value, quantity

parallel: year
"""

import pandas as pd
import os
from constants import *
from rca_utility import *
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time


def process_file(file):
    print(f"Processing {file} in thread: {threading.get_ident()}")

    file_name = os.path.join(FOLDER_PATH, file)
    df = pd.read_csv(file_name)
    selected_df = df[(df['i'].isin(COUNTRY_CODE)) & (df['j'].isin(COUNTRY_CODE))]

    year = file.split("_")[2][1:]

    country_all_rows = []
    for exporter in COUNTRY_CODE:
        for importer in COUNTRY_CODE:
            row = [year, importer, exporter]
            for val in VAL:
                xwj = selected_df[selected_df[importer] & selected_df[exporter]][val].sum()
                row.append(xwj)
            country_all_rows.apend(row)

    print(f"{file} is done.")
    
    return country_all_rows


if __name__ == "__main__":
    start_time = time.time()

    all_rows = []

    max_workers = os.cpu_count() * 2 if os.cpu_count() else 4 
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_file, file): file for file in os.listdir(FOLDER_PATH) if os.path.splitext(file)[0].split("_")[0] == "BACI"}
        
        for future in as_completed(futures):
            file = futures[future]
            try:
                data = future.result()
                all_rows.extend(data)
            except Exception as exc:
                print(f"{file} generated an exception: {exc}")

    world_all_rows = pd.DataFrame(all_rows, columns=['Year', 'Exporter', 'Importer', 'V', 'Q'])
    world_all_rows.to_csv("xwj.csv", index=False)

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Total execution time for xwj: {elapsed_time}")
