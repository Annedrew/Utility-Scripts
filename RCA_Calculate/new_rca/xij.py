"""
Aim to get xij.csv in parallel.

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

    rca = RCA()
    selected_df = rca.generate_xij(FOLDER_PATH, file, PROD, all_or_not=True)

    print(f"{file} is done.")

    return selected_df

    
if __name__ == "__main__":
    start_time = time.time()

    dfs = []
    max_workers = os.cpu_count() * 2 if os.cpu_count() else 4 
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_file, file): file for file in os.listdir(FOLDER_PATH) if os.path.splitext(file)[0].split("_")[0] == "BACI"}
        
        for future in as_completed(futures):
            file = futures[future]
            try:
                df = future.result()
                dfs.append(df)
            except Exception as exc:
                print(f"{file} generated an exception: {exc}")
        final_df = pd.concat(dfs, ignore_index=True)
        final_df = final_df.rename(columns={'t': 'Year', 'i': 'Exporter', 'j': 'Importer', 'k': 'Product', 'v': 'V', 'q': 'Q'})
        final_df.to_csv("xij.csv", index=False)

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Total execution time for xij: {elapsed_time}")
