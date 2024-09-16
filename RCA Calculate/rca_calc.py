# TODO: Check
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

    # country_single_rows = []
    country_all_rows = []
    world_single_rows = []
    world_all_rows = []

    for file in os.listdir(FOLDER_PATH):
        file_name = os.path.join(FOLDER_PATH, file)
        if os.path.splitext(file)[0].split("_")[0] == "BACI":
            df = pd.read_csv(file_name)
            countries = df['i'].unique()
            year = file.split("_")[2][1:]

            for val in VAL:
                # for prod in PROD:
                #     for country in countries:
                #         country_single_exp = rca.single_exp(df, val, prod, country)  # year, val, prod, country
                #         country_single_rows.append([year, country, prod, country_single_exp])

                for country in countries:
                    print(f"Calculating counrty exp, I am in {file}, {val}, {country}.")
                    country_all_exp = rca.all_exp(df, val, country)  # year, val, country
                    country_all_rows.append([year, country, country_all_exp])

                for prod in PROD:
                    print(f"Calculating world exp, I am in {file}, {val}, {prod}")
                    world_single_exp = rca.single_exp(df, val, prod, "all")  # year, val, prod
                    world_single_rows.append([year, prod, world_single_exp])

                world_all_exp = rca.all_exp(df, val, "all")  # year, val
                world_all_rows.append([year, world_all_exp])

    # country_single_df = pd.DataFrame(country_single_rows, columns=['Year', 'Country', 'Product', 'Val'])
    country_all_df = pd.DataFrame(country_all_rows, columns=['Year', 'Country', 'Val'])
    world_single_df = pd.DataFrame(world_single_rows, columns=['Year', 'Product', 'Val'])
    world_all_df = pd.DataFrame(world_all_rows, columns=['Year', 'Val'])

    # country_single_df.to_csv("Country_Single_Product_Export.csv", index=False)
    country_all_df.to_csv("Country_All_Product_Export.csv", index=False)
    world_single_df.to_csv("World_Single_Product_Export.csv", index=False)
    world_all_df.to_csv("World_All_Product_Export.csv", index=False)
