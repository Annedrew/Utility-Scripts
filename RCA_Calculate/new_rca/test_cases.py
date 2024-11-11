import pandas as pd
import numpy as np
from rca_utility import *
from constants import *


df_test = pd.DataFrame({
    'j': ['A', 'A', 'B', 'B'],  # importer
    'i': ['C', 'C', 'D', 'D'],  # exporter
    'v': [100, 150, 200, 250],
    'q': [10, 15, 20, 25]
})


def test_all_imp(df_test):
    rca = RCA()
    result_single = rca.all_imp(df_test, 'v', 'A', 'C')
    assert result_single == 250, f"Expected 250 but got {result_single}"

    result_all_exporters = rca.all_imp(df_test, 'v', 'A', 'all')
    assert result_all_exporters == 250, f"Expected 250 but got {result_all_exporters}"

    result_all_exporters_d = rca.all_imp(df_test, 'v', 'B', 'all')
    assert result_all_exporters_d == 450, f"Expected 450 but got {result_all_exporters_d}"


def test_generate_xwj(df_test):
    rca = RCA()
    result_all = rca.generate_xwj("RCA_Calculate/BACI_HS12_V202401b", "BACI_HS12_Y2023_V202401b_test.csv", ['v', 'q'], "all")
    
    expected_all = [
        ['2023', np.int64(4), np.int64(8), 2.844, 0.126], 
        ['2023', np.int64(4), np.int64(12), 1.185, 1.2], 
        ['2023', np.int64(4), np.int64(32), 1.8970000000000002, 0.172]]
    
    assert result_all == expected_all, f"Expected {expected_all} but got {result_all}"
    
    result_selected = rca.generate_xwj("RCA_Calculate/BACI_HS12_V202401b", "BACI_HS12_Y2023_V202401b_test.csv", ['v', 'q'], COUNTRY_CODE)
    
    expected_selected = [
        ['2023', 4, 4, 0.0, 0.0], 
        ['2023', 4, 8, 2.844, 0.126], 
        ['2023', 8, 4, 0.0, 0.0], 
        ['2023', 8, 8, 0.0, 0.0]]

    assert result_selected == expected_selected, f"Expected {expected_selected} but got {result_selected}"


test_generate_xwj(df_test)
# test_all_imp(df_test)
print("All tests passed!")
