import pandas as pd
import os
from constants import *
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time


def process_file(file):
    print(f"Processing {file} in thread: {threading.get_ident()}")
    rca = RCA()
    country_single_rows = []
    file_name = os.path.join(FOLDER_PATH, file)
    df = pd.read_csv(file_name)
    year = file.split("_")[2][1:]
    countries = pd.read_csv(file_name)['i'].unique()
    for country in countries:
        row = [year, country]
        for val in VAL:
            for prod in PROD:
                country_single_exp = rca.single_exp(df, val, prod, country)  # year, val, prod, country
                row.append(country_single_exp)
        country_single_rows.append(row)
    print(f"{file} is done.")

    return country_single_rows

class RCA:
    def single_exp(self, df, val, prod, country):
        if country != "all":
            single_prod = df[(df['k'] == prod) & (df['i'] == country)].copy()
        else:
            single_prod = df[df['k'] == prod].copy()

        if val == "v":
            column = 'v'
        elif val == "q":
            column = 'q'

        single_prod.loc[:, column] = pd.to_numeric(single_prod[column], errors='coerce').fillna(0)
        res = single_prod[column].sum(skipna=True)

        return float(res) if pd.notnull(res) else 0
    

    def all_exp(self, df, val, country):
        if country != "all":
            all_prod = df[df['i'] == country]
        else:
            all_prod = df

        if val == "v":
            column = 'v'
        elif val == "q":
            column = 'q'

        all_prod.loc[:, column] = pd.to_numeric(all_prod[column], errors='coerce').fillna(0)
        res = all_prod[column].sum(skipna=True)
        
        return res if pd.notnull(res) else 0


    def find_country_name(self, country_code, country_file):
        df_country = pd.read_csv(country_file)
        country_name = df_country[df_country['country_code'] == country_code]['country_name'].iloc[0]

        return country_name
    
    
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

    country_single_rows = pd.DataFrame(all_rows, columns=['Year', 'Country', 'V_121221', 'V_121229', 'Q_121221', 'Q_121229'])
    country_single_rows.to_csv("Country_Single_Product_Export.csv", index=False)

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Total execution time for country_single_exp: {elapsed_time}")