# country_all.py calculate the value of country i’s total exports.

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
    countries = df["i"].unique() # exporter
    year = file.split("_")[2][1:]

    rca = RCA()
    country_all_rows = []
    for country in countries:
        row = [year, country]
        for val in VAL:
            country_all_exp = rca.all_exp(df, val, country)  # year, val, country
            row.append(country_all_exp)
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

    country_all_rows = pd.DataFrame(all_rows, columns=['Year', 'Country', 'V', 'Q'])
    country_all_rows.to_csv("Country_All_Product_Export.csv", index=False)

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Total execution time for country_all_exp: {elapsed_time}")
