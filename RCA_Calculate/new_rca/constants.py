import os
import pandas as pd

FOLDER_PATH = os.path.join(os.getcwd(), "RCA_Calculate", "BACI_HS12_V202401b")

COUNTRY_FILE = os.path.join(FOLDER_PATH, "country_codes_V202401b.csv")

PRODUCT_FILE = os.path.join(FOLDER_PATH, "product_codes_HS12_V202401b.csv")

PROD = [121221, 121229]

VAL = ['V', 'Q']

# -------- Selected Countries -------- 

# COUNTRIES = ['China', 'Norway', 'Denmark', 'India', 'Italy']

# COUNTRY_CODE = [152, 156, 360, 604]

# COUNTRY_PAIR = [(152, 152), (152, 156), (152, 360), (152, 604), (156, 152), (156, 156), (156, 360), (156, 604), (360, 152), (360, 156), (360, 360), (360, 604), (604, 152), (604, 156), (604, 360), (604, 604)]

COUNTRY_CODE = [156, 579, 208, 699, 380]

# COUNTRY_CODE = [4, 8]  # for testing purposes, normally, comment out this line.

# COUNTRY_PAIR = [(156, 156), (156, 579), (156, 208), (156, 699), (156, 380), (579, 156), (579, 579), (579, 208), (579, 699), (579, 380), (208, 156), (208, 579), (208, 208), (208, 699), (208, 380), (699, 156), (699, 579), (699, 208), (699, 699), (699, 380), (380, 156), (380, 579), (380, 208), (380, 699), (380, 380)]
