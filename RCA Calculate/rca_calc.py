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


    def transform_countries(self, output_file, country_file):
        df_output = pd.read_csv(output_file)
        df_country = pd.read_csv(country_file)

        mapping_dict = pd.Series(df_country.country_name.values, index=df_country.country_code).to_dict()
        
        df_output['i'] = df_output['i'].map(mapping_dict)
        df_output['j'] = df_output['j'].map(mapping_dict)

        output_file_path = f'{os.path.split(output_file)[0]}/output_countries.csv'
        df_output.to_csv(output_file_path, index=False)
    
    
if __name__ == "__main__":
    rca = RCA()

    # This is not smart
    df = pd.DataFrame(columns=["Year", "Country", "RCA-USD-121221", "RCA-TONNE-121221", "RCA-USD-121229", "RCA-TONNE-121229"])

    row = []

    for file in os.listdir(FOLDER_PATH):
        file_name = os.path.join(FOLDER_PATH, file)
        if os.path.splitext(file)[1] == ".csv" and file_name != PRODUCT_FILE:

            for val in VAL:
                world_all_exp = rca.all_exp(file_name, val, "all")  # year, val
                if world_all_exp == 0:
                    print(f"world_all_exp is 0 in file: {file}")
                else:
                    
                    for prod in PROD:
                        world_single_exp = rca.single_exp(file_name, val, prod, "all")  # year, val, prod
                        if world_single_exp == 0:
                            print(f"world_single_exp is 0 in file: {file}")

                        for country in COUNTRY_FILE:
                            country_all_exp = rca.all_exp(file_name, val, country)  # year, val, country
                            if country_all_exp == 0:
                                print(f"country_all_exp is 0 in file: {file}")
                            country_single_exp = rca.single_exp(file_name, val, prod, country)  # year, val, prod, country
                            if country_single_exp == 0:
                                print(f"country_single_exp is 0 in file: {file}")

        print(f"{file} is operated.")