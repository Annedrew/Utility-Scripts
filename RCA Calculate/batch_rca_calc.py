import pandas as pd
import os
from constants import *

class RCA:
    def single_exp(self, df, val, prod, country):
        if country != "all":
            single_prod = df[(df['k'] == prod) & (df['i'] == country)]
        else:
            single_prod = df[df['k'] == prod]

        if val == "v":
            res = single_prod['v'].sum(skipna=True)
        elif val == "q":
            res = single_prod['q'].sum(skipna=True)

        return res if pd.notnull(res) else 0
    

    def all_exp(self, df, val, country):
        if country != "all":
            all_prod = df[df['i'] == country]
        else:
            all_prod = df

        if val == "v":
            res = all_prod['v'].sum(skipna=True)
        elif val == "q":
            res = all_prod['q'].sum(skipna=True)

        return res if pd.notnull(res) else 0


    def find_country_name(self, country_code, country_file):
        df_country = pd.read_csv(country_file)
        country_name = df_country[df_country['country_code'] == country_code]['country_name'].iloc[0]

        return country_name
    
    
if __name__ == "__main__":
    rca = RCA()

    country_single_rows = []
    country_all_rows = []
    world_single_rows = []
    world_all_rows = []

    BATCH_SIZE = 10000
    for file in os.listdir(FOLDER_PATH):
        file_name = os.path.join(FOLDER_PATH, file)
        if os.path.splitext(file)[0].split("_")[0] == "BACI":
            for df in pd.read_csv(file_name, chunksize=BATCH_SIZE):
                countries = df['i'].unique()
                year = file.split("_")[2][1:]

                for val in VAL:
                    for country in countries:
                        country_all_exp = rca.all_exp(df, val, country)  # year, val, country
                        country_all_rows.append([year, country, country_all_exp])

    country_all_df = pd.DataFrame(country_all_rows, columns=['Year', 'Country', 'Val'])
    country_all_df.to_csv("Country_All_Product_Export.csv", index=False)