import pandas as pd
import numpy as np
from rca_utility import *
from constants import *


def test_all_imp():
    rca = RCA()
    df_test = pd.read_csv("RCA_Calculate/BACI_HS12_V202401b_test/BACI_HS12_Y2023_V202401b_test.csv")

    result_single = rca.all_imp(df_test, 'v', 8, 4)
    assert result_single == 2.844, f"Expected 2.844 but got {result_single}"

    result_all_exporters = rca.all_imp(df_test, 'v', 32, 'all')
    assert result_all_exporters == 1.8970000000000002, f"Expected 1.8970000000000002 but got {result_all_exporters}"

    result_all_exporters_q = rca.all_imp(df_test, 'q', 12, 'all')
    assert result_all_exporters_q == 1.2, f"Expected 1.2 but got {result_all_exporters_q}"

    result_all_exporters_q2 = rca.all_imp(df_test, 'q', 32, 'all')
    assert result_all_exporters_q2 == 0.172, f"Expected 0.172 but got {result_all_exporters_q2}"


def test_generate_xwj():
    rca = RCA()
    result_all = rca.generate_xwj("RCA_Calculate/BACI_HS12_V202401b_test", "BACI_HS12_Y2023_V202401b_test.csv", ['v', 'q'], "all")

    expected_all = [
        ['2023', np.int64(4), np.int64(8), 2.844, 0.126], 
        ['2023', np.int64(4), np.int64(12), 1.185, 1.2], 
        ['2023', np.int64(4), np.int64(32), 1.8970000000000002, 0.172], 
        ['2023', np.int64(4), np.int64(20), 0.0, 0.0], 
        ['2023', np.int64(8), np.int64(8), 0.0, 0.0], 
        ['2023', np.int64(8), np.int64(12), 0.0, 0.0], 
        ['2023', np.int64(8), np.int64(32), 0.0, 0.0], 
        ['2023', np.int64(8), np.int64(20), 0.787, 0.19]
    ]
    
    assert result_all == expected_all, f"Expected {expected_all} but got {result_all}"
    
    result_selected = rca.generate_xwj("RCA_Calculate/BACI_HS12_V202401b_test", "BACI_HS12_Y2023_V202401b_test.csv", ['v', 'q'], COUNTRY_CODE)
    
    expected_selected = [
        ['2023', 4, 4, 0.0, 0.0], 
        ['2023', 4, 8, 2.844, 0.126], 
        ['2023', 8, 4, 0.0, 0.0], 
        ['2023', 8, 8, 0.0, 0.0]
    ]

    assert result_selected == expected_selected, f"Expected {expected_selected} but got {result_selected}"


test_all_imp()
test_generate_xwj()
print("All tests passed!")
