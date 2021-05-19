import numpy
import pandas as pd
import os
import common
from pandasql import sqldf
from bag_coronavirus import credentials
from bag_coronavirus import vmdl_extract

file_path = vmdl_extract.retrieve_vmdl_data()
#file_path = os.path.join(credentials.vmdl_path, credentials.vmdl_file)

print(f'Reading data into dataframe...')
df = pd.read_csv(file_path, sep=';')
# df['vacc_date_dt'] = pd.to_datetime(df.vacc_date, format='%Y-%m-%dT%H:%M:%S.%f%z')
df['vacc_day'] = df.vacc_date.str.slice(stop=10)

print(f'Executing calculations...')
pysqldf = lambda q: sqldf(q, globals())

print(f'Filter by BS and vacc_date...')
df_bs = sqldf('''
    select * 
    from df 
    where person_residence_ctn = "BS" and vacc_day < strftime("%Y-%m-%d", "now", "localtime")''')

print(f'Calculating age groups...')
bins =      [numpy.NINF, 15,     49,         64,         74,         numpy.inf]
labels =    ['Unbekannt',       '16-49',    '50-64',    '65-74',    '> 74']
df_bs['age_group'] = pd.cut(df_bs.person_age, bins=bins, labels=labels, include_lowest=True)

df_crosstab = pd.crosstab(df_bs.vacc_day, df_bs.age_group).sort_values(by='vacc_day', ascending=False)

print(f'Adding days without vaccinations...')
df_all_days = pd.DataFrame(data=pd.date_range(start=df_bs.vacc_day.min(), end=df_bs.vacc_day.max()).astype(str), columns=['vacc_day'])
df_all_days = df_all_days.set_index('vacc_day', drop=False)

df_crosstab_all = df_crosstab.join(df_all_days, how='outer').fillna(0)

export_file_name = os.path.join(credentials.vmdl_path, f'vaccination_report_bs_age_group.csv')
print(f'Exporting resulting data to {export_file_name}...')
df_crosstab_all.to_csv(export_file_name, index=False)
# common.upload_ftp(export_file_name, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'bag/vmdl')
print(f'Job successful!')