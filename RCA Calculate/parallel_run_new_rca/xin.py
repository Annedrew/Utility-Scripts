import pandas as pd
import os
from constants import *
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time


class RCA:
    # import single prod
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
    

    # import all prod
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
    
"""
output:
    columns: year, exporter, importer, product, value
"""
if __name__ == "__main__":
    start_time = time.time()
    
    df = pd.read_csv(f"{os.getcwd()}/RCA Calculate/parallel_run_new_rca/xij.csv", sep=",")

    rca = RCA()
    val = "V"
    exp_country = "all"
    columns = []
    for y in df['t'].unique():
        for imp_country in COUNTRY_CODE:
            columns.append[df.loc]
            for prod in PROD:
                rca.single_imp(df, val, prod, imp_country, exp_country)

    
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Total execution time for country_all_exp: {elapsed_time}")
