import pandas as pd
import os
from constants import *


class RCA:
    def single_exp(self, file_path, val, prod, country):
        df = pd.read_csv(file_path)
        if country != "all":
            single_prod = df[(df['k'] == prod) & (df['i'] == country)]
            
            if val == "v":
                res = single_prod['v'].sum()
            elif val == "q":
                res = single_prod['q'].sum()
        else:
            single_prod = df[df['k'] == prod]

            if val == "v":
                res = single_prod['v'].sum()
            elif val == "q":
                res = single_prod['q'].sum()

        return res
    

    def all_exp(self, file_path, val, country):
        df = pd.read_csv(file_path)

        if country != "all":
            all_prod = df[df['i'] == country]

            if val == "v":
                res = all_prod['v'].sum()
            elif val == "q":
                res = all_prod['q'].sum()
        else:
            if val == "v":
                res = df['v'].sum()
            elif val == "q":
                res = df['q'].sum()

        return res


    def find_country_name(self, country_code, country_file):
        df_country = pd.read_csv(country_file)
        country_name = df_country[df_country['country_code'] == country_code]['country_name']

        return country_name

    
if __name__ == "__main__":
    rca = RCA()

    zero_df = pd.DataFrame(columns=['File', 'Country Code', 'Country Name', 'Product'])

    # Calculate country_single_exp
    rows = []
    country_single_df = pd.DataFrame(columns=['Year', 'Country', 'Product', 'Val'])
    for file in os.listdir(FOLDER_PATH):
        file_name = os.path.join(FOLDER_PATH, file)
        if os.path.splitext(file)[0].split("_")[0] == "BACI":
            countries = pd.read_csv(file_name)['i'].unique()
            for val in VAL:
                for prod in PROD:
                    for country in countries:
                        country_single_exp = rca.single_exp(file_name, val, prod, country)  # year, val, prod, country
                        print(f"country_single_exp: {country_single_exp} ")
                        year = file.split("_")[2][1:]
                        rows.append([year, country, prod, country_single_exp])
        print(f"{file} is operated.")
    
    country_single_df = pd.DataFrame(rows, columns=['Year', 'Country', 'Product', 'Val'])
    country_single_df.to_csv("Country_Single_Product_Export.csv", index=False)
    print(f"Country_Single_Product_Export.csv is done.")


    # Calculate country_all_exp:
    rows = []
    country_all_df = pd.DataFrame(columns=['Year', 'Country', 'Val'])
    for file in os.listdir(FOLDER_PATH):
        file_name = os.path.join(FOLDER_PATH, file)
        if os.path.splitext(file)[0].split("_")[0] == "BACI":
            countries = pd.read_csv(file_name)['i'].unique()
            for val in VAL:
                for country in countries:
                    country_all_exp = rca.all_exp(file_name, val, country)  # year, val, country
                    print(f"country_all_exp: {country_all_exp} ")
                    year = file.split("_")[2][1:]
                    rows.append([year, country, country_all_exp])
        print(f"{file} is operated.")

    country_all_df = pd.DataFrame(rows, columns=['Year', 'Country', 'Val'])
    country_all_df.to_csv("Country_All_Product_Export.csv", index=False)
    print(f"Country_All_Product_Export.csv is done.")


    # Calculate world_single_exp:
    rows = []
    world_singe_df = pd.DataFrame(columns=['Year', 'Product', 'Val'])
    for file in os.listdir(FOLDER_PATH):
        file_name = os.path.join(FOLDER_PATH, file)
        if os.path.splitext(file)[0].split("_")[0] == "BACI":
            countries = pd.read_csv(file_name)['i'].unique()
            for val in VAL:
                for prod in PROD:
                    world_single_exp = rca.single_exp(file_name, val, prod, "all")  # year, val, prod
                    print(f"world_single_exp: {world_single_exp} ")
                    year = file.split("_")[2][1:]
                    rows.append([year, prod, world_single_exp])
        print(f"{file} is operated.")

    world_singe_df = pd.DataFrame(rows, columns=['Year', 'Product', 'Val'])
    world_singe_df.to_csv("World_Single_Product_Export.csv", index=False)
    print(f"World_Single_Product_Export.csv is done.")


    # Calculate world_all_exp:
    rows = []
    world_all_df = pd.DataFrame(columns=['Year', 'Val'])
    for file in os.listdir(FOLDER_PATH):
        file_name = os.path.join(FOLDER_PATH, file)
        if os.path.splitext(file)[0].split("_")[0] == "BACI":
            countries = pd.read_csv(file_name)['i'].unique()
            for val in VAL:
                world_all_exp = rca.all_exp(file_name, val, "all")  # year, val
                print(f"world_all_exp: {world_all_exp} ")
                year = file.split("_")[2][1:]
                rows.append([year, world_single_exp])
        print(f"{file} is operated.")

    world_all_df = pd.DataFrame(rows, columns=['Year', 'Val'])
    world_all_df.to_csv("World_All_Product_Export.csv", index=False)
    print(f"World_All_Product_Export.csv is done.")
