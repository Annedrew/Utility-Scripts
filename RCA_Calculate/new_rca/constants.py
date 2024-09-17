import os

FOLDER_PATH = f"{os.getcwd()}/RCA_Calculate/BACI_HS12_V202401b"

COUNTRY_FILE = f"{FOLDER_PATH}/country_codes_V202401b.csv"

PRODUCT_FILE = f"{FOLDER_PATH}/product_codes_HS12_V202401b.csv"

PROD = [121221, 121229]

VAL = ['V', 'Q']

# -------- Selected Countries -------- 

COUNTRIES = ['China', 'Norway', 'Denmark', 'India', 'Italy']

COUNTRY_CODE = [156, 579, 208, 699, 380]

COUNTRY_PAIR = [(156, 156), (156, 579), (156, 208), (156, 699), (156, 380), (579, 156), (579, 579), (579, 208), (579, 699), (579, 380), (208, 156), (208, 579), (208, 208), (208, 699), (208, 380), (699, 156), (699, 579), (699, 208), (699, 699), (699, 380), (380, 156), (380, 579), (380, 208), (380, 699), (380, 380)]