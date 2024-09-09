import pandas as pd
import os
from constants import *


class RCA:
    def single_exp(self, file_path, country, val, prod):
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
    

    def all_exp(self, file_path, country, val):
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
    
    country_file = f"{FILE_PATH}/country_codes_V202401b.csv"

    # This is not smart
    df = pd.DataFrame(columns=["Year", "Country", "RCA-USD-121221", "RCA-TONNE-121221", "RCA-USD-121229", "RCA-TONNE-121229"])

    for file in os.listdir(FILE_PATH):
        if os.path.splitext(file)[1] == ".csv" and file != "product_codes_HS12_V202401b.csv":
            for country in country_file:
                row = []
                for val in VAL:
                    for prod in PROD:
                        file_name = os.path.join(FILE_PATH, file)
                        world_single_exp = rca.single_exp(file_name, "all", val, prod)
                        country_all_exp = rca.all_exp(file_name, "all", val, prod)
                        denominator = world_single_exp/country_all_exp
                        country_single_exp = rca.single_exp(file_name, country, val, prod)
                        country_all_exp = rca.all_exp(file_name, country, val, prod)

                        row.append((country_single_exp/country_all_exp)/denominator)
                
                df_row = pd.DataFrame([row])
                pd.concat([df, df_row], ignore_index=True)
                print(f"{country} {file} is added.")

        print(f"{file} is operated.")
    
    df.to_csv("RCA.csv", index=False)