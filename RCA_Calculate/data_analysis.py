"""
estimate how many calculations need to be done.
"""
# import pandas as pd
# df = pd.read_csv("RCA Calculate/BACI_HS12_V202401b/country_codes_V202401b.csv")
# print(len(df))  # 238
# print(238 * 2 * 2 * 10)  # country * val * prod * year

"""
investigate is every file include all the countries in country file.  - No
"""
# import pandas as pd
# country = "RCA Calculate/BACI_HS12_V202401b/country_codes_V202401b.csv"
# data = "RCA Calculate/BACI_HS12_V202401b/BACI_HS12_Y2012_V202401b.csv"
# a_df = pd.read_csv(country)
# b_df = pd.read_csv(data)
# a_values = a_df['country_code'].unique()
# b_values = b_df['i'].unique() 
# a_values = set(a_values)
# b_values = set(b_values)
# not_include = a_values - b_values
# print(not_include)
# print(len(not_include))


"""
Check if the document has the same country for each file - No
"""
# previous_countries = None
# countries = set(pd.read_csv(file_name)['i'].unique())
# if previous_countries is None:
#     previous_countries = countries
# else:
#     if countries != previous_countries:
#         print("Not the same.")


# Two ways to solve this
# 1. parallel process - OK
# 2. batch - Works, but need to sort before setup batches, same country might end up in different batch.

"""
How many lines in a file?
"""
# [("BACI_HS12_Y2021_V202401b.csv", 11567369), ("BACI_HS12_Y2017_V202401b.csv", 11133430), 
# ("BACI_HS12_Y2020_V202401b.csv", 11061172), ("BACI_HS12_Y2016_V202401b.csv", 10824942), 
# ("BACI_HS12_Y2018_V202401b.csv", 11275332), ("BACI_HS12_Y2019_V202401b.csv", 11423408), 
# ("BACI_HS12_Y2022_V202401b.csv", 11326567), ("BACI_HS12_Y2014_V202401b.csv", 10205793), 
# ("BACI_HS12_Y2013_V202401b.csv", 9787005), ("BACI_HS12_Y2012_V202401b.csv", 9012146), 
# ("BACI_HS12_Y2015_V202401b.csv", 10781842)]
# import pandas as pd
# import os
# for file in os.listdir("RCA Calculate/BACI_HS12_V202401b"):
#     if file.split("_")[0] == "BACI":
#         df = pd.read_csv(os.path.join(os.getcwd(), "RCA Calculate/BACI_HS12_V202401b", file))
#         print(file, len(df))
