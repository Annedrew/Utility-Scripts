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
    world_single_rows = []
    file_name = os.path.join(FOLDER_PATH, file)
    df = pd.read_csv(file_name)
    year = file.split("_")[2][1:]
    row = [year]
    for val in VAL:
        for prod in PROD:
            world_single_exp = rca.single_exp(df, val, prod, "all")  # year, val, prod
            row.append(world_single_exp)
    world_single_rows.append(row)
    print(f"{file} is done.")

    return world_single_rows


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
    
    world_single_rows = pd.DataFrame(all_rows, columns=['Year', 'V_121221', 'V_121229', 'Q_121221', 'Q_121229'])
    world_single_rows.to_csv("World_Single_Product_Export.csv", index=False)

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Total execution time for world_single_exp: {elapsed_time}")
