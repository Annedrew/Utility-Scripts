import pandas as pd
from constants import *
from rca_utility import *
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading


def process_rca_calc(val, prod, file_path_list):
    print(f"Processing in thread: {threading.get_ident()}")
    rca = RCA()
    df = rca.rca_calc(val, prod, *file_path_list)
    
    return df


if __name__ == "__main__":
    start_time = time.time()

    file_path_list = [f"{os.getcwd()}/results/Country_Single_Product_Export.csv", 
                      f"{os.getcwd()}/results/Country_All_Product_Export.csv", 
                      f"{os.getcwd()}/results/World_Single_Product_Export.csv", 
                      f"{os.getcwd()}/results/World_All_Product_Export.csv"]
    
    max_workers = os.cpu_count() * 2 if os.cpu_count() else 4 
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_rca_calc, val, prod, file_path_list): (val, prod) for val in VAL for prod in PROD}
        
        dfs = [pd.read_csv(f"{os.getcwd()}/results/Country_Single_Product_Export.csv", dtype={'Year': int, 'Country': int}).iloc[:, :2]]
        for future in as_completed(futures):
            df = future.result()
            dfs.append(df)

    final_df = pd.concat(dfs, axis=1)
    final_df.to_csv("rca.csv", index=False)

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Total execution time for world_all_exp: {elapsed_time}")
