import logging
import shutil
import pandas as pd
import os
import common
from pandasql import sqldf
from bag_coronavirus import credentials
from bag_coronavirus.src import vmdl
import common.change_tracking as ct
import ods_publish.etl_id as odsp


def main():
    vmdl_copy_path = vmdl.file_path().replace('vmdl.csv', 'vmdl_impf_uebersicht.csv')
    logging.info(f'Copying vmdl csv for this specific job to {vmdl_copy_path}...')
    shutil.copy(vmdl.file_path(), vmdl_copy_path)
    if not ct.has_changed(vmdl_copy_path):
        logging.info(f'Data have not changed, doing nothing for this dataset: {vmdl_copy_path}')
    else:
        df = extract_data(vmdl_copy_path)
        df_export = transform_data(df)
        export_file_name = load_data(df_export)
        if not ct.has_changed(export_file_name):
            logging.info(f'Data have not changed, doing nothing for this dataset: {export_file_name}')
        else:
            common.upload_ftp(export_file_name, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass,'bag/vmdl')
            odsp.publish_ods_dataset_by_id('100111')
    logging.info(f'Job successful!')


def extract_data(file_path):
    logging.info(f'Reading data from into dataframe {file_path}...')
    df = pd.read_csv(file_path, sep=';')
    return df


def transform_data(df):
    logging.info(f'Executing calculations...')
    # df['vacc_date_dt'] = pd.to_datetime(df.vacc_date, format='%Y-%m-%dT%H:%M:%S.%f%z')
    df['vacc_day'] = df.vacc_date.str.slice(stop=10)
    pysqldf = lambda q: sqldf(q, globals())
    logging.info(f'Filter by BS and vacc_date, sum type 1 and 99, create "other" type, count persons...')
    df_bs = sqldf(f'''
        select * 
        from df 
        where reporting_unit_location_ctn = "BS" and vacc_day < "{vmdl.today_string()}"''')
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
    logging.info(f'Create empty table of all combinations...')
    df_all_days = pd.DataFrame(data=pd.date_range(start=df_bs.vacc_day.min(), end=vmdl.yesterday_string()).astype(str), columns=['vacc_day'])
    df_all_vacc_count = sqldf('select distinct vacc_count from df;')
    df_all_location_type = sqldf('select distinct location_type from df_bs_by')
    df_all_comb = sqldf('select * from df_all_days cross join df_all_vacc_count cross join df_all_location_type;')
    logging.info(f'Adding days without vaccinations...')
    df_bs_by_all = df_all_comb.merge(df_bs_by, on=['vacc_day', 'vacc_count', 'location_type'], how='outer').fillna(0)
    logging.info(f'Pivoting...')
    df_pivot_table = df_bs_by_all.pivot_table(values='count', index=['vacc_day'], columns=['location_type', 'vacc_count'], fill_value=0)
    # Replace the 2-level column names with a string that concatenates both strings
    df_pivot_table.columns = ["_".join(str(c) for c in col) for col in df_pivot_table.columns.values]
    df_pivot = df_pivot_table.reset_index()
    logging.info(f'Ensure columns exist...')
    for column_name in [
        'hosp_3',
        'vacc_centre_3',
        'other_1',
        'other_2',
        'other_3',
        'in_aph_verabreichte_impfungen_pro_tag',
        'im_aph_mit_erster_dosis_geimpfte_personen_pro_tag',
        'im_aph_mit_zweiter_dosis_geimpfte_personen_pro_tag',
        'im_aph_mit_dritter_dosis_geimpfte_personen_pro_tag',
    ]:
        if column_name not in df_pivot.columns:
            df_pivot[column_name] = 0
    logging.info(f'Calculating columns...')
    df_pivot['hosp'] = df_pivot.hosp_1 + df_pivot.hosp_2 + df_pivot.hosp_3
    df_pivot['vacc_centre'] = df_pivot.vacc_centre_1 + df_pivot.vacc_centre_2 + df_pivot.vacc_centre_3
    df_pivot['other'] = df_pivot.other_1 + df_pivot.other_2 + df_pivot.other_3
    df_pivot['vacc_count_1'] = df_pivot.hosp_1 + df_pivot.vacc_centre_1 + df_pivot.other_1
    df_pivot['vacc_count_2'] = df_pivot.hosp_2 + df_pivot.vacc_centre_2 + df_pivot.other_2
    df_pivot['vacc_count_3'] = df_pivot.hosp_3 + df_pivot.vacc_centre_3 + df_pivot.other_3
    df_pivot['cum_1'] = df_pivot.vacc_count_1.cumsum()
    df_pivot['cum_2'] = df_pivot.vacc_count_2.cumsum()
    df_pivot['cum_3'] = df_pivot.vacc_count_3.cumsum()
    df_pivot['only_1'] = df_pivot.cum_1 - df_pivot.cum_2
    df_pivot['total'] = df_pivot.hosp + df_pivot.vacc_centre + df_pivot.other
    df_pivot['total_cum'] = df_pivot.total.cumsum()
    logging.info(f'Renaming and restricting columns for export...')
    df_export = df_pivot.rename(columns={
        'vacc_day': 'datum',
        'hosp_1': 'im_spital_mit_erster_dosis_geimpfte_personen_pro_tag',
        'hosp_2': 'im_spital_mit_zweiter_dosis_geimpfte_personen_pro_tag',
        'hosp_3': 'im_spital_mit_dritter_dosis_geimpfte_personen_pro_tag',
        'vacc_centre_1': 'im_impfzentrum_mit_erster_dosis_geimpfte_personen_pro_tag',
        'vacc_centre_2': 'im_impfzentrum_mit_zweiter_dosis_geimpfte_personen_pro_tag',
        'vacc_centre_3': 'im_impfzentrum_mit_dritter_dosis_geimpfte_personen_pro_tag',
        'other_1': 'anderswo_mit_erster_dosis_geimpfte_personen_pro_tag',
        'other_2': 'anderswo_mit_zweiter_dosis_geimpfte_personen_pro_tag',
        'other_3': 'anderswo_mit_dritter_dosis_geimpfte_personen_pro_tag',
        'hosp': 'im_spital_verabreichte_impfungen_pro_tag',
        'vacc_centre': 'im_impfzentrum_verabreichte_impfungen_pro_tag',
        'other': 'anderswo_verabreichte_impfungen_pro_tag',
        'vacc_count_1': 'total_mit_erster_dosis_geimpfte_personen_pro_tag',
        'vacc_count_2': 'total_mit_zweiter_dosis_geimpfte_personen_pro_tag',
        'vacc_count_3': 'total_mit_dritter_dosis_geimpfte_personen_pro_tag',
        'cum_1': 'total_personen_mit_erster_dosis',
        'cum_2': 'total_personen_mit_zweiter_dosis',
        'cum_3': 'total_personen_mit_dritter_dosis',
        'only_1': 'total_personen_mit_ausschliesslich_erster_dosis',
        'total': 'total_verabreichte_impfungen_pro_tag',
        'total_cum': 'total_verabreichte_impfungen',
    })
    df_export = df_export[[
        'datum',
        'total_verabreichte_impfungen',
        'total_personen_mit_erster_dosis',
        'total_personen_mit_ausschliesslich_erster_dosis',
        'total_personen_mit_zweiter_dosis',
        'total_personen_mit_dritter_dosis',
        'im_impfzentrum_verabreichte_impfungen_pro_tag',
        'im_impfzentrum_mit_erster_dosis_geimpfte_personen_pro_tag',
        'im_impfzentrum_mit_zweiter_dosis_geimpfte_personen_pro_tag',
        'im_impfzentrum_mit_dritter_dosis_geimpfte_personen_pro_tag',
        'in_aph_verabreichte_impfungen_pro_tag',
        'im_aph_mit_erster_dosis_geimpfte_personen_pro_tag',
        'im_aph_mit_zweiter_dosis_geimpfte_personen_pro_tag',
        'im_aph_mit_dritter_dosis_geimpfte_personen_pro_tag',
        'im_spital_verabreichte_impfungen_pro_tag',
        'im_spital_mit_erster_dosis_geimpfte_personen_pro_tag',
        'im_spital_mit_zweiter_dosis_geimpfte_personen_pro_tag',
        'im_spital_mit_dritter_dosis_geimpfte_personen_pro_tag',
        'anderswo_verabreichte_impfungen_pro_tag',
        'anderswo_mit_erster_dosis_geimpfte_personen_pro_tag',
        'anderswo_mit_zweiter_dosis_geimpfte_personen_pro_tag',
        'anderswo_mit_dritter_dosis_geimpfte_personen_pro_tag',
        'total_verabreichte_impfungen_pro_tag',
    ]]
    return df_export


def load_data(df_export: pd.DataFrame) -> str:
    export_file_name = os.path.join(credentials.vmdl_path, f'vaccination_report_bs.csv')
    logging.info(f'Exporting resulting data to {export_file_name}...')
    df_export.to_csv(export_file_name, index=False)
    return export_file_name


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
