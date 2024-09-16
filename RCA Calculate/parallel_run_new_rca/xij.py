import pandas as pd
import os
from constants import *
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time


# Export value from commodity 121221&121229 from a country to country j
"""
output:
    columns: year, exporter, importer, product, value, quantity
"""
def process_file(file):
    print(f"Processing {file} in thread: {threading.get_ident()}")

    rca = RCA()
    rows = []
    file_name = os.path.join(FOLDER_PATH, file)
    df = pd.read_csv(file_name)
    selected_df = df[(df['i'].isin(COUNTRY_CODE)) & (df['j'].isin(COUNTRY_CODE)) & (df['k'].isin(PROD))]

    print(f"{file} is done.")

    return selected_df


class RCA:
    def single_imp(self, df, val, prod, imp_country, exp_country):
        if exp_country != "all":
            single_prod = df[(df['k'] == prod) & (df['j'] == imp_country) & (df['i'] == exp_country)].copy()
        else:
            single_prod = df[(df['k'] == prod) & (df['j'] == imp_country)].copy()

        if val == "V":
            column = 'v'
        elif val == "Q":
            column = 'q'

        single_prod.loc[:, column] = pd.to_numeric(single_prod[column], errors='coerce').fillna(0)
        res = single_prod[column].sum(skipna=True)

        return float(res) if pd.notnull(res) else 0
    

    def all_imp(self, df, val, imp_country, exp_country):
        if exp_country != "all":
            all_prod = df[(df['i'] == exp_country) & (df['j'] == imp_country)].copy()
        else:
            all_prod = df[df['j'] == imp_country].copy()

        if val == "V":
            column = 'v'
        elif val == "Q":
            column = 'q'

        all_prod.loc[:, column] = pd.to_numeric(all_prod[column], errors='coerce').fillna(0)
        res = all_prod[column].sum(skipna=True)
        
        return float(res) if pd.notnull(res) else 0


    def find_country_name(self, country_code, country_file):
        df_country = pd.read_csv(country_file)
        country_name = df_country[df_country['country_code'] == country_code]['country_name'].iloc[0]

        return country_name
    
    
if __name__ == "__main__":
    start_time = time.time()

    dfs = []
    max_workers = os.cpu_count() * 2 if os.cpu_count() else 4 
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_file, file): file for file in os.listdir(FOLDER_PATH) if os.path.splitext(file)[0].split("_")[0] == "BACI"}
        
        for future in as_completed(futures):
            df = future.result()
            dfs.append(df)
        final_df = pd.concat(dfs, ignore_index=True)
        final_df.to_csv("xij.csv", index=False)

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Total execution time for country_all_exp: {elapsed_time}")
