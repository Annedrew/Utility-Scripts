from constants import *

pair_country_list = []
for imp in COUNTRY_CODE:
    for exp in COUNTRY_CODE:
        pair_country_list.append((imp, exp))

print(pair_country_list)