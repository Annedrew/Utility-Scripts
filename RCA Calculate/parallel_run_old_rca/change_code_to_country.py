import pandas as pd
import os

# Transform code to countries
def transform_countries(output_file, country_file):
    df_output = pd.read_csv(output_file)
    df_country = pd.read_csv(country_file)

    mapping_dict = pd.Series(df_country.country_name.values, index=df_country.country_code).to_dict()
    
    df_output['i'] = df_output['i'].map(mapping_dict)
    # df_output['j'] = df_output['j'].map(mapping_dict)

    output_file_path = f'{os.path.split(output_file)[0]}/output_countries.csv'
    df_output.to_csv(output_file_path, index=False)

transform_countries("RCA Calculate/parallel_run_old_rca/rca.csv", "RCA Calculate/BACI_HS12_V202401b/country_codes_V202401b.csv")
