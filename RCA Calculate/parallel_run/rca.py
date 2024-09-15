import pandas as pd
from constants import *
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading


def process_rca_calc(val, prod, file_path_list):
    print(f"Processing in thread: {threading.get_ident()}")
    rca = RCA()
    df = rca.rca_calc(val, prod, *file_path_list)
    
    return df


class RCA:
    def rca_formular_old(self, c_s, c_a, w_s, w_a):
        rca = (float(c_s) / float(c_a)) / (float(w_s) / float(w_a))

        rca = round(rca, 2)

        return rca

    def rca_calc(self, val, prod, country_single_file, country_all_file, world_single_file, world_all_file):
        c_single = pd.read_csv(country_single_file, sep=",")
        c_all = pd.read_csv(country_all_file, sep=",")
        w_single = pd.read_csv(world_single_file, sep=",")
        w_all = pd.read_csv(world_all_file, sep=",")
        row_num = len(c_single)
        col_name = f"{val}_{prod}"
        rca_scores = []
        for i in range(row_num):
            c_s = c_single.loc[i, col_name] # float
            c_a = c_all[(c_all['Country'] == c_single.loc[i, 'Country']) & (c_all['Year'] == c_single.loc[i, 'Year'])]['V'].values[0] # find by country and year
            w_s = w_single[w_single['Year'] == c_single.loc[i, 'Year']][col_name].values[0] # find by product and year
            w_a = w_all[w_all['Year'] == c_single.loc[i, 'Year']]['V'].values[0] # filter by year
            
            rca_scores.append(self.rca_formular_old(c_s, c_a, w_s, w_a))
        
        df = pd.DataFrame(rca_scores, columns=[col_name])
        
        return df


    def find_country_name(self, country_code, country_file):
        df_country = pd.read_csv(country_file)
        country_name = df_country[df_country['country_code'] == country_code]['country_name'].iloc[0]

        return country_name
    
    
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

    final_df = pd.concat(dfs, axis=1, ignore_index=True)
    final_df.to_csv("rca.csv", index=False)

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Total execution time for world_all_exp: {elapsed_time}")
