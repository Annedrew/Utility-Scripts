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

    # This is not smart
    rca_df = pd.DataFrame(columns=["Year", "Country", "RCA-USD-121221", "RCA-TONNE-121221", "RCA-USD-121229", "RCA-TONNE-121229"])
    zero_df = pd.DataFrame(columns=['File', 'Country Code', 'Country Name', 'Product'])

    row = []

    countries = pd.read_csv(COUNTRY_FILE)['country_code']


    for file in os.listdir(FOLDER_PATH):
        file_name = os.path.join(FOLDER_PATH, file)
        if os.path.splitext(file)[1] == ".csv" and file_name != PRODUCT_FILE:

            for val in VAL:
                world_all_exp = rca.all_exp(file_name, val, "all")  # year, val
                if world_all_exp == 0:
                    rca_res = float('nan')
                    print(f"world_all_exp is 0 in file: {file}")
                else:

                    for prod in PROD:
                        world_single_exp = rca.single_exp(file_name, val, prod, "all")  # year, val, prod
                        if world_single_exp == 0:
                            rca_res = float('nan')
                            print(f"world_single_exp is 0 in file: {file}")
                        else:
                            for country in countries:
                                country_all_exp = rca.all_exp(file_name, val, country)  # year, val, country
                                if country_all_exp == 0:
                                    rca_res = float('nan')
                                    print(f"country_all_exp {country} is 0 in file: {file}")
                                else:
                                    country_single_exp = rca.single_exp(file_name, val, prod, country)  # year, val, prod, country
                                    if country_single_exp == 0:
                                        rca_res = 0
                                        print(f"country_single_exp {country} is 0 in file: {file}")
                                        zero_df['File'] = f"{file}"
                                        zero_df['Country Code'] = f"{country}"
                                        zero_df['Country Name'] = rca.find_country_name(country, COUNTRY_FILE)
                                        zero_df['Product'] = f"{prod}"
                                    else:
                                        # TODO: Final rca calculation phase, when the row has 'nan' or '0' value, fill it with 'nan' or '0' for now.
                                        if rca_res == float('nan') or rca_res == 0:  
                                            row = [rca_res for i in range(len(rca_df.columns))]
                                            pd.concat([rca_df, row], ignore_index=True)
                                        else:
                                            rca_res = (country_single_exp / country_all_exp) / (world_single_exp / world_all_exp)
                                            pd.concat([rca_df, row], ignore_index=True)


        zero_df.to_csv("zero_export_info.csv", index=False)

        print(f"{file} is operated.")