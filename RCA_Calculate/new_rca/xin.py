"""
xin: Total export value of commodity i from all exporting countries to country j.

output file columns: year, importer, product, value, quantity

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
    countries = df["j"].unique() # importer
    year = file.split("_")[2][1:]

    rca = RCA()
    country_all_rows = []
    for country in countries:
        row = [year, country]
        for val in VAL:
            for prod in PROD:
                country_all_imp = rca.single_imp(df, val, prod, country, "all")
                row.append(country_all_imp)
        country_all_rows.append(row)

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

    country_all_rows = pd.DataFrame(all_rows, columns=['Year', 'Country', 'V_121221', 'V_121229', 'Q_121221', 'Q_121229'])
    country_all_rows.to_csv("xin.csv", index=False)


    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Total execution time for xin: {elapsed_time}")
