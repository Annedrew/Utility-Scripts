# Idea
# 1. Find the needed lines.
# 2. Find for all files.
# 3. Save the result.
# 4. Transform data into countries

import pandas as pd
import os


class DataExtract:
    # Find product and save it to csv file
    def find_product(self, file_name):
        print(f"Extracting file: {file_name}")

        delimiters = [',', ';', '\t', '|']

        for delimiter in delimiters:
            try:
                df = pd.read_csv(file_name, sep=delimiter)
                product_lines = df[df["k"].isin([121220, 121221, 121229])]
                print(f"Extracting finished with {file_name}")

                return product_lines
            except pd.errors.ParserError:
                pass
            except Exception as e:
                pass

        return None


    # Save csv file
    def save_csv(self, product_lines, folder_path):
        output_file = f"{folder_path}/output.csv"
        if os.path.exists(output_file):
            existing_data = pd.read_csv(output_file)
            combined_data = pd.concat([existing_data, product_lines], ignore_index=True)
        else:
            combined_data = product_lines

        combined_data.to_csv(output_file, index=False)
        print("Extracted data saved.")
        print("---------------------")


    # Transform code to countries
    def transform_countries(self, output_file, country_file):
        df_output = pd.read_csv(output_file)
        df_country = pd.read_csv(country_file)

        mapping_dict = pd.Series(df_country.country_name.values, index=df_country.country_code).to_dict()
        
        df_output['i'] = df_output['i'].map(mapping_dict)
        df_output['j'] = df_output['j'].map(mapping_dict)

        output_file_path = f'{os.path.split(output_file)[0]}/output_countries.csv'
        df_output.to_csv(output_file_path, index=False)


if __name__ == "__main__":
    folder_path = "/Users/bp45th/Downloads/BACI_HS92_V202401b"
    
    extract = DataExtract()

    for file in os.listdir(folder_path):
        if os.path.splitext(file)[1] == ".csv":
            file_name = os.path.join(folder_path, file)
            product_data = extract.find_product(file_name)
            extract.save_csv(product_data, folder_path)

    print("Extracting finished.")

    extract.transform_countries(f"{folder_path}/output.csv", f"{folder_path}/country_codes_V202401b.csv")

