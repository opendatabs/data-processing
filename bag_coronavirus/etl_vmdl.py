import pandas as pd
import requests
import os
import common
from pandasql import sqldf
from bag_coronavirus import credentials

payload_token = f'client_id={credentials.vmdl_client_id}&scope={credentials.vmdl_scope}&username={credentials.vmdl_user}&password={credentials.vmdl_password}&grant_type=password'
headers_token = {'Content-Type': 'application/x-www-form-urlencoded'}
print(f'Getting OAUTH2 access token...')
resp_token = requests.request("POST", credentials.vmdl_url_token, headers=headers_token, data=payload_token)
resp_token.raise_for_status()
# token_type = resp_token.json()['token_type']
auth_string = f'Bearer {resp_token.json()["access_token"]}'

payload_download={}
headers_download = {'Authorization': auth_string}
print(f'Downloading data...')
resp_download = common.requests_get(credentials.vmdl_url_download, headers=headers_download, data=payload_download)
resp_download.raise_for_status()
file_path = os.path.join(credentials.vmdl_path, credentials.vmdl_file)
print(f'Writing data to file {file_path}...')
resp_download.encoding = 'utf-8'
with open(file_path, "w") as f:
    f.write(resp_download.text)

print(f'Reading data into dataframe...')
df = pd.read_csv(file_path, sep=';')
# df['vacc_date_dt'] = pd.to_datetime(df.vacc_date, format='%Y-%m-%dT%H:%M:%S.%f%z')
df['vacc_day'] = df.vacc_date.str.slice(stop=10)

print(f'Executing calculations...')
pysqldf = lambda q: sqldf(q, globals())

print(f'Sum type 1 and 99, filter by BS, count persons...')
df_bs = sqldf('''
    select * 
    from df 
    where reporting_unit_location_ctn = "BS"''')

df_bs_by = sqldf('''
    select vacc_day, vacc_count, 
    case reporting_unit_location_type 
        when 1  then "vacc_centre" 
        when 99 then "vacc_centre" 
        when 6  then "hosp" 
        else "other" 
        end as location_type, 
    count(person_anonymised_id) as count 
    from df_bs 
    group by vacc_day, vacc_count, location_type
    order by vacc_day asc;''')

print(f'Create empty table of all combinations...')
df_all_days = pd.DataFrame(data=pd.date_range(start=df_bs.vacc_day.min(), end=df_bs.vacc_day.max()).astype(str), columns=['vacc_day'])
df_all_vacc_count = sqldf('select distinct vacc_count from df;')
df_all_location_type = sqldf('select distinct location_type from df_bs_by')
df_all_comb = sqldf('select *from df_all_days cross join df_all_vacc_count cross join df_all_location_type;')

print(f'Adding days without vaccinations...')
df_bs_by_all = df_all_comb.merge(df_bs_by, on=['vacc_day', 'vacc_count', 'location_type'], how='outer').fillna(0)

print(f'Pivoting...')
df_pivot_table = df_bs_by_all.pivot_table(values='count', index=['vacc_day'], columns=['location_type', 'vacc_count'], fill_value=0)
# Replace the 2-level column names with a string that concatenates both strings
df_pivot_table.columns = ["_".join(str(c) for c in col) for col in df_pivot_table.columns.values]
df_pivot = df_pivot_table.reset_index()

print(f'Ensure columns exist...')
for column_name in [
    'other_1',
    'other_2',
    'in_aph_verabreichte_impfungen_pro_tag',
    'im_aph_mit_erster_dosis_geimpfte_personen_pro_tag',
    'im_aph_mit_zweiter_dosis_geimpfte_personen_pro_tag',
]:
    if column_name not in df_pivot.columns:
        df_pivot[column_name] = 0

print(f'Calculating columns...')
df_pivot['hosp'] = df_pivot.hosp_1 + df_pivot.hosp_2
df_pivot['vacc_centre'] = df_pivot.vacc_centre_1 + df_pivot.vacc_centre_2
df_pivot['other'] = df_pivot.other_1 + df_pivot.other_2
df_pivot['vacc_count_1'] = df_pivot.hosp_1 + df_pivot.vacc_centre_1 + df_pivot.other_1
df_pivot['vacc_count_2'] = df_pivot.hosp_2 + df_pivot.vacc_centre_2 + df_pivot.other_2
df_pivot['cum_1'] = df_pivot.vacc_count_1.cumsum()
df_pivot['cum_2'] = df_pivot.vacc_count_2.cumsum()
df_pivot['only_1'] = df_pivot.cum_1 - df_pivot.cum_2
df_pivot['total'] = df_pivot.hosp + df_pivot.vacc_centre + df_pivot.other
df_pivot['total_cum'] = df_pivot.total.cumsum()

print(f'Renaming and restricting columns for export...')
df_export = df_pivot.rename(columns={
    'vacc_day':         'datum',
    'hosp_1':           'im_spital_mit_erster_dosis_geimpfte_personen_pro_tag',
    'hosp_2':           'im_spital_mit_zweiter_dosis_geimpfte_personen_pro_tag',
    'vacc_centre_1':    'im_impfzentrum_mit_erster_dosis_geimpfte_personen_pro_tag',
    'vacc_centre_2':    'im_impfzentrum_mit_zweiter_dosis_geimpfte_personen_pro_tag',
    'other_1':          'anderswo_mit_erster_dosis_geimpfte_personen_pro_tag',
    'other_2':          'anderswo_mit_zweiter_dosis_geimpfte_personen_pro_tag',
    'hosp':             'im_spital_verabreichte_impfungen_pro_tag',
    'vacc_centre':      'im_impfzentrum_verabreichte_impfungen_pro_tag',
    'other':            'anderswo_verabreichte_impfungen_pro_tag',
    'vacc_count_1':     'total_mit_erster_dosis_geimpfte_personen_pro_tag',
    'vacc_count_2':     'total_mit_zweiter_dosis_geimpfte_personen_pro_tag',
    'cum_1':            'total_personen_mit_erster_dosis',
    'cum_2':            'total_personen_mit_zweiter_dosis',
    'only_1':           'total_personen_mit_ausschliesslich_erster_dosis',
    'total':            'total_verabreichte_impfungen_pro_tag',
    'total_cum':        'total_verabreichte_impfungen',
})

df_export = df_export[[
    'datum',
    'total_verabreichte_impfungen',
    'total_personen_mit_erster_dosis',
    'total_personen_mit_ausschliesslich_erster_dosis',
    'total_personen_mit_zweiter_dosis',
    'im_impfzentrum_verabreichte_impfungen_pro_tag',
    'im_impfzentrum_mit_erster_dosis_geimpfte_personen_pro_tag',
    'im_impfzentrum_mit_zweiter_dosis_geimpfte_personen_pro_tag',
    'in_aph_verabreichte_impfungen_pro_tag',
    'im_aph_mit_erster_dosis_geimpfte_personen_pro_tag',
    'im_aph_mit_zweiter_dosis_geimpfte_personen_pro_tag',
    'im_spital_verabreichte_impfungen_pro_tag',
    'im_spital_mit_erster_dosis_geimpfte_personen_pro_tag',
    'im_spital_mit_zweiter_dosis_geimpfte_personen_pro_tag',
    'anderswo_verabreichte_impfungen_pro_tag',
    'anderswo_mit_erster_dosis_geimpfte_personen_pro_tag',
    'anderswo_mit_zweiter_dosis_geimpfte_personen_pro_tag',
    'total_verabreichte_impfungen_pro_tag',
]]

export_file_name = os.path.join(credentials.vmdl_path, f'vaccination_report_bs.csv')
print(f'Exporting resulting data to {export_file_name}...')
df_export.to_csv(export_file_name, index=False)
common.upload_ftp(export_file_name, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'bag/vmdl')
print(f'Job successful!')