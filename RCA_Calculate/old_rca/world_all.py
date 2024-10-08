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
    world_all_rows = []
    file_name = os.path.join(FOLDER_PATH, file)
    df = pd.read_csv(file_name)
    year = file.split("_")[2][1:]
    row = [year]
    for val in VAL:
        world_all_exp = rca.all_exp(df, val,"all")  # year, val
        row.append(world_all_exp)
    world_all_rows.append(row)
    print(f"{file} is done.")
    
    return world_all_rows

    
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

    world_all_rows = pd.DataFrame(all_rows, columns=['Year', 'V', 'Q'])
    world_all_rows.to_csv("World_All_Product_Export.csv", index=False)

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Total execution time for world_all_exp: {elapsed_time}")
