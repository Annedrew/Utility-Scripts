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
        """
        Return the sum of a single production (prod) export to a single country or the world (country). The sum can be value or quantity (val).
        """
        if country != "all":
            single_prod = df[(df['k'] == prod) & (df['i'] == country)].copy()
        else:
            single_prod = df[df['k'] == prod].copy()

        single_prod_df = pd.to_numeric(single_prod[val], errors='coerce').fillna(0)
        res = single_prod_df.sum(skipna=True)

        return float(res) if pd.notnull(res) else 0
    

    def all_exp(self, df, val, country):
        """
        Return the sum of all production export to a single country or the world (country). The sum can be value or quantity (val).
        """
        if country != "all":
            all_prod = df[df['i'] == country]
        else:
            all_prod = df

        all_prod_df = pd.to_numeric(all_prod[val], errors='coerce').fillna(0)
        res = all_prod_df.sum(skipna=True)
        
        return float(res) if pd.notnull(res) else 0


    def single_imp(self, df, val, prod, imp_country, exp_country):
        """
        Return the sum of a single production (prod) from importer (imp_country) to a single exporter or the world (exp_country). The sum can be value or quantity (val).
        """
        if exp_country != "all":
            single_prod = df[(df['k'] == prod) & (df['j'] == imp_country) & (df['i'] == exp_country)].copy()
        else:
            single_prod = df[(df['k'] == prod) & (df['j'] == imp_country)].copy()

        single_prod_df = pd.to_numeric(single_prod[val], errors='coerce').fillna(0)
        res = single_prod_df.sum(skipna=True)

        return float(res) if pd.notnull(res) else 0
    

    def all_imp(self, df, val, imp_country, exp_country):
        """
        Return the sum of all production from importer (imp_country) to a single exporter or the world (exp_country). The sum can be value or quantity (val).
        """
        if exp_country != "all":
            all_prod = df[(df['i'] == exp_country) & (df['j'] == imp_country)].copy()
        else:
            all_prod = df[df['j'] == imp_country].copy()

        all_prod_df = pd.to_numeric(all_prod[val], errors='coerce').fillna(0)
        res = all_prod_df.sum(skipna=True)
        
        return float(res) if pd.notnull(res) else 0
    

    def generate_xij(self, folder_path, file, prods, all_or_not):
        """
        Generate xij.csv file for final calculation. 
            'xij' means export value of commodity i from a country to country j.

        xij.csv columns: 
            year, exporter, importer, product, value, quantity
        """
        file_name = os.path.join(folder_path, file)
        df = pd.read_csv(file_name)

        if all_or_not == True:
            selected_df = df[df['k'].isin(prods)]
        else:
            country_code = COUNTRY_CODE # selected importers
            selected_df = df[(df['i'].isin(country_code)) & (df['j'].isin(country_code)) & (df['k'].isin(prods))]

        return selected_df
    

    def generate_xin(self, folder_path, file, vals, prods):
        """
        Generate xin.csv file for final calculation. 
            'xin' means total export value of commodity i from all exporting countries to country j.

        xin.csv columns: 
            year, importer, product, value, quantity
        """
        file_name = os.path.join(folder_path, file)
        df = pd.read_csv(file_name)
        importers = df["j"].unique()
        year = file.split("_")[2][1:]

        country_all_rows = []
        for importer in importers:
            for prod in prods:
                row = [year, importer, prod]
                for val in vals:
                    country_all_imp = self.single_imp(df, val, prod, importer, "all")
                    row.append(country_all_imp)
                country_all_rows.append(row)
        
        return country_all_rows


    def generate_xwj(self, folder_path, file, vals, all_or_not):
        """
        Generate xwj.csv file for final calculation. 
            'xwj' means total export value of all commodities from a country to country j.

        xwj.csv columns: 
            year, exporter, importer, value, quantity
        """
        file_name = os.path.join(folder_path, file)
        df = pd.read_csv(file_name)
        year = file.split("_")[2][1:]
        country_all_rows = []
        
        if all_or_not == True:
            importer_code = df['j'].unique() # all importers
            exporter_code = df['i'].unique() # all exporters
            selected_df = df
            
            for exporter in exporter_code:
                for importer in importer_code:
                    row = [year, exporter, importer]
                    for val in vals:
                        country_single_imp = self.all_imp(selected_df, val, importer, exporter)
                        row.append(country_single_imp)
                    country_all_rows.append(row)
        else:
            country_code = COUNTRY_CODE # selected importers
            selected_df = df[(df['i'].isin(country_code)) & (df['j'].isin(country_code))]

            for exporter in country_code:
                for importer in country_code:
                    row = [year, exporter, importer]
                    for val in vals:
                        country_single_imp = self.all_imp(selected_df, val, importer, exporter)
                        row.append(country_single_imp)
                    country_all_rows.append(row)

        return country_all_rows
        

    def generate_xwn(self, folder_path, file, vals):
        """
        Generate xwn.csv file for final calculation. 
            'xwn' means total export value of all commodities from all exporting to country j.

        xwn.csv columns: 
            year, importer, value, quantity
        """
        file_name = os.path.join(folder_path, file)
        df = pd.read_csv(file_name)
        importers = df['j'].unique()
        year = file.split("_")[2][1:]
        
        world_all_rows = []
        for importer in importers:
            row = [year, importer]
            for val in vals:
                world_all_exp = self.all_imp(df, val, importer, "all")
                row.append(world_all_exp)
            world_all_rows.append(row)

        return world_all_rows


    def rca_formular(self, xij, xin, xwj, xwn):
        """
        Math formular for RCA calculation.
        """
        if str(xij).strip()== "NA":
            xij = 0
        if xij !=0 and xin != 0 and xwj != 0 and xwn != 0:
            rca = (float(xij) / float(xin)) / (float(xwj) / float(xwn))
        else:
            rca = None

        # rca = round(rca, 3)

        return rca


    def rca_calc_new(self, val, xij, xin, xwj, xwn):
        """
        Batch calculation of RCA with new RCA formular.
        """
        xij_df = pd.read_csv(xij, sep=",")
        xin_df = pd.read_csv(xin, sep=",")
        xwj_df = pd.read_csv(xwj, sep=",")
        xwn_df = pd.read_csv(xwn, sep=",")

        row_num = len(xij_df)
        rca_scores = []
        for i in range(row_num):
            xij = xij_df.loc[i, val]
            xin = xin_df[(xin_df['Year'] == xij_df.loc[i, 'Year']) & (xin_df['Importer'] == xij_df.loc[i, 'Importer']) & (xin_df['Product'] == xij_df.loc[i, 'Product'])][val].values[0]
            xwj = xwj_df[(xwj_df['Year'] == xij_df.loc[i, 'Year']) & (xwj_df['Importer'] == xij_df.loc[i, 'Importer']) & (xwj_df['Exporter'] == xij_df.loc[i, 'Exporter'])][val].values[0]
            xwn = xwn_df[(xwn_df['Year'] == xij_df.loc[i, 'Year']) & (xwn_df['Importer'] == xij_df.loc[i, 'Importer'])][val].values[0]

            rca_scores.append(self.rca_formular(xij, xin, xwj, xwn))
        
        df = pd.DataFrame(rca_scores)
        df.columns = [val]
        
        return df


    def rca_calc_old(self, val, prod, country_single_file, country_all_file, world_single_file, world_all_file):
        """
        Batch calculation of RCA with old RCA formular.
        """
        c_single = pd.read_csv(country_single_file, sep=",")
        c_all = pd.read_csv(country_all_file, sep=",")
        w_single = pd.read_csv(world_single_file, sep=",")
        w_all = pd.read_csv(world_all_file, sep=",")

        row_num = len(c_single)
        col_name = f"{val}_{prod}"
        rca_scores = []
        for i in range(row_num):
            c_s = c_single.loc[i, col_name] # float
            c_a = c_all[(c_all['Country'] == c_single.loc[i, 'Country']) & (c_all['Year'] == c_single.loc[i, 'Year'])][val].values[0] # find by country and year
            w_s = w_single[w_single['Year'] == c_single.loc[i, 'Year']][col_name].values[0] # find by product and year
            w_a = w_all[w_all['Year'] == c_single.loc[i, 'Year']][val].values[0] # filter by year
            
            rca_scores.append(self.rca_formular(c_s, c_a, w_s, w_a))
        
        df = pd.DataFrame(rca_scores, columns=[col_name])
        
        return df


    def find_country_name(self, country_code, country_file):
        """
        Parse a country code to a country name.
        """
        df_country = pd.read_csv(country_file)
        country_name = df_country[df_country['country_code'] == country_code]['country_name'].iloc[0]

        return country_name


    def transform_countries(self, country_code_file, country_file, country_column_name):
        """
        Convert 'country code column' into 'country name column' of a file (country_code_file).
        """
        df_file = pd.read_csv(country_code_file)
        df_country = pd.read_csv(country_file)

        mapping_dict = pd.Series(df_country.country_name.values, index=df_country.country_code).to_dict()
        
        df_file[country_column_name] = df_file[country_column_name].map(mapping_dict)

        df_file.to_csv(f"{os.path.splitext(country_code_file)[0]}_(country name).csv", index=False)


    def generate_country_pair(self, country_code):
        """
        Generate country pair. If country_code = [1,2], output will be [(1,1), (1,2), (2,1), (2,2)]
        """
        pair_country_list = []

        for imp in country_code:
            for exp in country_code:
                pair_country_list.append((imp, exp))

        return pair_country_list
    