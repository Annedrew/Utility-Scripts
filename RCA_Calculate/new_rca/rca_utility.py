'''
 # @ Author: Ning An
 # @ Create Time: 2024-09-16 22:29:19
 # @ Modified by: Ning An
 # @ Modified time: 2024-09-17 09:55:52
 '''

import pandas as pd
from constants import *


class RCA:
    def single_exp(self, df, val, prod, country):
        if country != "all":
            single_prod = df[(df['k'] == prod) & (df['i'] == country)].copy()
        else:
            single_prod = df[df['k'] == prod].copy()

        if val == "V":
            column = 'v'
        elif val == "Q":
            column = 'q'

        single_prod_df = pd.to_numeric(single_prod[column], errors='coerce').fillna(0)
        res = single_prod_df.sum(skipna=True)

        return float(res) if pd.notnull(res) else 0
    

    def all_exp(self, df, val, country):
        if country != "all":
            all_prod = df[df['i'] == country]
        else:
            all_prod = df

        if val == "V":
            column = 'v'
        elif val == "Q":
            column = 'q'

        all_prod_df = pd.to_numeric(all_prod[column], errors='coerce').fillna(0)
        res = all_prod_df.sum(skipna=True)
        
        return float(res) if pd.notnull(res) else 0


    def single_imp(self, df, val, prod, imp_country, exp_country):
        if exp_country != "all":
            single_prod = df[(df['k'] == prod) & (df['j'] == imp_country) & (df['i'] == exp_country)].copy()
        else:
            single_prod = df[(df['k'] == prod) & (df['j'] == imp_country)].copy()

        if val == "V":
            column = 'v'
        elif val == "Q":
            column = 'q'

        single_prod_df = pd.to_numeric(single_prod[column], errors='coerce').fillna(0)
        res = single_prod_df.sum(skipna=True)

        return float(res) if pd.notnull(res) else 0
    

    def all_imp(self, df, val, imp_country, exp_country):
        if exp_country != "all":
            all_prod = df[(df['i'] == exp_country) & (df['j'] == imp_country)].copy()
        else:
            all_prod = df[df['j'] == imp_country].copy()

        if val == "V":
            column = 'v'
        elif val == "Q":
            column = 'q'

        all_prod_df = pd.to_numeric(all_prod[column], errors='coerce').fillna(0)
        res = all_prod_df.sum(skipna=True)
        
        return float(res) if pd.notnull(res) else 0


    def rca_formular_old(self, c_s, c_a, w_s, w_a):
        rca = (float(c_s) / float(c_a)) / (float(w_s) / float(w_a))

        # rca = round(rca, 3)

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


    def transform_countries(self, output_file, country_file, country_column_name):
        df_output = pd.read_csv(output_file)
        df_country = pd.read_csv(country_file)

        mapping_dict = pd.Series(df_country.country_name.values, index=df_country.country_code).to_dict()
        
        df_output[country_column_name] = df_output[country_column_name].map(mapping_dict)

        # output_file_path = f'{os.path.split(output_file)[0]}/output_countries.csv'
        df_output.to_csv("country.csv", index=False)


    def generate_country_pair(self):
        pair_country_list = []

        for imp in COUNTRY_CODE:
            for exp in COUNTRY_CODE:
                pair_country_list.append((imp, exp))

        return pair_country_list