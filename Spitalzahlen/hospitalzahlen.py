"""
check which day it is, differentiate between monday and other weekdays, variables: day

get data of the day at 9:15, and the weekend if it's Monday, variables: day, data

if no data there, send email, check again after 15 minutes and get data, if still no data, give warning

make dataframe with data of the day (+weekend), variables in:day, data ; variables out: day, data frame

Select latest entry of each day, variables in: day, dataframe, variables out: day, data row

Calculate needed numbers: variabels in: day, data row ; variables out: day, new data row

[Betten_frei_Normal, Betten_frei_Normal_COVID, Betten_frei_IMCU, Betten_frei_IPS_ohne_Beatmung,
      Betten_frei_IPS_mit_Beatmung, Betten_frei_ECMO, Betten_belegt_Normal, Betten_belegt_IMCU, Betten_belegt_IPS_ohne_Beatmung,
      Betten_belegt_IPS_mit_Beatmung, Betten_belegt_ECMO] = calculate_numbers(ies_numbers)

enter numbers in CoReport
"""
import pandas as pd
import get_data
import send_email
import calculation


df = get_data.make_dataframe()
df_coreport = calculation.calculate_numbers(df)
pd.set_option('display.max_columns', None)
print(df_coreport.head())
