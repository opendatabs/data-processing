import os
import pandas as pd
import logging

from staka_kandidaturen import credentials
import common
import ods_publish.etl_id as odsp


def main():
    # Grossratswahlen 2024
    df_gr = process_grossrat()
    path_export = os.path.join(credentials.path_data, 'export', '100385_kandidaturen_grossrat.csv')
    df_gr.to_csv(path_export, index=False)
    common.upload_ftp(path_export, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass,
                      '/wahlen_abstimmungen/wahlen/gr/kandidaturen_2024')
    odsp.publish_ods_dataset_by_id('100385')

    # Regierungsratswahlen 2024
    df_rr = process_regierungsrat()
    path_export = os.path.join(credentials.path_data, '100386_kandidaturen_regierungsrat.csv')
    df_rr.to_csv(path_export, index=False)
    common.upload_ftp(path_export, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass,
                      '/wahlen_abstimmungen/wahlen/rr/kandidaturen_2024')
    odsp.publish_ods_dataset_by_id('100386')

    # Regierungspräsidiumswahlen 2024
    df_rp = process_regierungsrat(which='RP')
    path_export = os.path.join(credentials.path_data, '100387_kandidaturen_regierungspraesidium.csv')
    df_rp.to_csv(path_export, index=False)
    common.upload_ftp(path_export, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass,
                      '/wahlen_abstimmungen/wahlen/rr/kandidaturen_2024')
    odsp.publish_ods_dataset_by_id('100387')

    # Don't run since already done
    """
    # Regierungsrat-Ersatzwahl 2024
    df_rr = process_regierungsrat()
    path_export = os.path.join(credentials.path_data, '100333_kandidaturen_regierungsrat_ersatz.csv')
    df_rr.to_csv(path_export, index=False)
    common.upload_ftp(path_export, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass,
                      '/wahlen_abstimmungen/wahlen/rr_ersatz/kandidaturen_2024')
    odsp.publish_ods_dataset_by_id('100333')

    # Regierungspräsidium-Ersatzwahl 2024
    df_rp = process_regierungsrat(which='RP')
    path_export = os.path.join(credentials.path_data, '100334_kandidaturen_regierungspraesidium_ersatz.csv')
    df_rp.to_csv(path_export, index=False)
    common.upload_ftp(path_export, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass,
                      '/wahlen_abstimmungen/wahlen/rr_ersatz/kandidaturen_2024')
    odsp.publish_ods_dataset_by_id('100334')

    # Nationalratswahlen 2023
    df_nr = process_nationalrat()
    path_export = os.path.join(credentials.path_data, '100316_kandidaturen_nationalrat.csv')
    df_nr.to_csv(path_export, index=False)
    common.upload_ftp(path_export, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass,
                      '/wahlen_abstimmungen/wahlen/nr/kandidaturen_2023')
    odsp.publish_ods_dataset_by_id('100316')

    # Ständeratswahlen 2023
    df_sr = process_staenderat()
    path_export = os.path.join(credentials.path_data, '100317_kandidaturen_staenderat.csv')
    df_sr.to_csv(path_export, index=False)
    common.upload_ftp(path_export, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass,
                      '/wahlen_abstimmungen/wahlen/sr/kandidaturen_2023')
    odsp.publish_ods_dataset_by_id('100317')
    """


def process_nationalrat() -> pd.DataFrame:
    file_kb = os.path.join(credentials.path_orig, "NR_für_KB_ergänzt.xlsx")
    file_mm = os.path.join(credentials.path_orig, "NR_Kandidaturen_für_Medienmitteilung.xlsx")

    xlsx_kb = pd.ExcelFile(file_kb)
    xlsx_mm = pd.ExcelFile(file_mm)

    df_all_kand = pd.DataFrame(index=None)
    for list_party in xlsx_kb.sheet_names:
        df_list = pd.read_excel(xlsx_kb, sheet_name=list_party, skiprows=3, dtype=str)
        df_list.columns = ['kand_nr', 'vorname', 'name', 'bisher', 'jahrgang', 'kurzbeschrieb', 'wh. in', 'wh_in']
        df_with_header = pd.read_excel(xlsx_kb, sheet_name=list_party, dtype=str)
        hlvs = float('NaN') if isinstance(df_with_header.iloc[(1, 1)], float) \
            else ', '.join(x.strip().zfill(2) for x in df_with_header.iloc[(1, 1)].split(','))
        ulvs = float('NaN') if isinstance(df_with_header.iloc[(2, 1)], float) \
            else ', '.join(x.strip().zfill(2) for x in df_with_header.iloc[(2, 1)].split(','))
        for _, row in df_list.iterrows():
            entries_per_kand = {
                'listenbezeichnung': df_with_header.iloc[(0, 0)],
                'hlv_mit': hlvs,
                'hlv_link': float('NaN') if isinstance(hlvs, float) else f"https://data.bs.ch/explore/dataset/100316/?refine.listen_nr={'&refine.listen_nr='.join([list_party.split('_')[0]] + hlvs.split(', '))}",
                'ulv_mit': ulvs,
                'ulv_link': float('NaN') if isinstance(ulvs, float) else f"https://data.bs.ch/explore/dataset/100316/?refine.listen_nr={'&refine.listen_nr='.join([list_party.split('_')[0]] + ulvs.split(', '))}",
                'kand_nr': row.kand_nr,
                'bisher': row.bisher,
                'name_vorname': f"{row['name']}, {row.vorname}",
                'name': row['name'],
                'vorname': row.vorname,
                'jahrgang': row.jahrgang,
                'kurzbeschrieb': row.kurzbeschrieb,
                'wh_in': row.wh_in
            }
            df_all_kand = pd.concat([df_all_kand, pd.DataFrame([entries_per_kand])])

    # For assigning gender, list number and list name
    df_all_kand_gend = pd.DataFrame(index=None)
    for list_party in xlsx_mm.sheet_names:
        df_list = pd.read_excel(xlsx_mm, sheet_name=list_party, dtype=str)
        df_list = df_list.drop(index=0)
        for _, row in df_list.iterrows():
            entries_per_kand = {
                'kand_nr': row['Kand. Nr.'],
                'listen_nr': list_party.split('_', 1)[0],
                'listenkurzbezeichnung': '_'.join(list_party.split('_', 1)[1:]),
                'geschlecht': row.Geschlecht
            }
            df_all_kand_gend = pd.concat([df_all_kand_gend, pd.DataFrame([entries_per_kand])])

    df_nr = pd.merge(df_all_kand, df_all_kand_gend, on='kand_nr')
    return df_nr


def process_staenderat() -> pd.DataFrame:
    file_sr = os.path.join(credentials.path_orig, "SR_für_KB.xlsx")
    xlsx_sr = pd.ExcelFile(file_sr)

    df_all_kand = pd.DataFrame(index=None)
    for sheet_name in xlsx_sr.sheet_names:
        df_list = pd.read_excel(xlsx_sr, sheet_name=sheet_name, skiprows=4, dtype=str)
        df_list.columns = ['listen_nr', 'name', 'vorname', 'bisher', 'jahrgang', 'kurzbeschrieb']
        df_with_header = pd.read_excel(xlsx_sr, sheet_name=sheet_name, dtype=str)
        entries_per_kand = {
            'listen_nr': sheet_name.split('_', 1)[0],
            'listenbezeichnung': df_with_header.iloc[(2, 0)],
            'bisher': df_list.bisher[0],
            'name_vorname': f"{df_list['name'][0]}, {df_list.vorname[0]}",
            'name': df_list['name'][0],
            'vorname': df_list.vorname[0],
            'geschlecht': 'f' if sheet_name == '01_Herzog' else 'm',
            'jahrgang': df_list.jahrgang[0],
            'kurzbeschrieb': df_list.kurzbeschrieb[0],
        }
        df_all_kand = pd.concat([df_all_kand, pd.DataFrame([entries_per_kand])])

    return df_all_kand


def process_regierungsrat(which='RR'):
    xlsx = os.path.join(credentials.path_orig, f"{which}_für_OGD.xlsx")

    df = pd.DataFrame(index=None)
    for sheet_name in pd.ExcelFile(xlsx).sheet_names:
        df_list = pd.read_excel(xlsx, sheet_name=sheet_name, dtype=str)
        df_list.columns = ['listen_nr', 'listenbezeichnung', 'zeilen_nr', 'name', 'vorname',
                           'bisher', 'geschlecht', 'jahrgang', 'zusatz']
        df = pd.concat([df, df_list])
    df['name_vorname'] = df['name'] + ', ' + df['vorname']
    df['listen_nr'] = df['listen_nr'].apply(lambda x: x.zfill(2))
    df = df.drop(columns=['zeilen_nr'])
    return df


def process_grossrat():
    df = pd.DataFrame(index=None)
    wahlkreise = [('GBO', 'Grossbasel-Ost'), ('GBW', 'Grossbasel-West'), ('KB', 'Kleinbasel'),
                  ('RI', 'Riehen'), ('BE', 'Bettingen')]

    for wahlkreis in wahlkreise:
        xlsx = os.path.join(credentials.path_orig, f"{wahlkreis[0]}_für_OGD.xlsx")
        for sheet_name in pd.ExcelFile(xlsx).sheet_names:
            df_list = pd.read_excel(xlsx, sheet_name=sheet_name, skiprows=2, dtype=str)
            df_list.columns = ['listen_nr', 'listenkurzbezeichnung', 'listenbezeichnung', 'kand_nr',
                               'name', 'vorname', 'bisher', 'geschlecht', 'jahrgang', 'zusatz']
            df_list['listen_nr'] = df_list['listen_nr'].astype(str).apply(lambda x: x.zfill(2))
            df_list['wahlkreis'] = wahlkreis[1]
            df_list['name_vorname'] = df_list['name'] + ', ' + df_list['vorname']
            df = pd.concat([df, df_list])

    return df


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful')