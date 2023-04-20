import pandas as pd
import openpyxl
from datetime import datetime
import logging
import os
import pathlib
from common import change_tracking as ct
import common
import ods_publish.etl_id as odsp
from bvb_fahrgastzahlen import credentials


def main():
    sheets = pd.read_excel(os.path.join(pathlib.Path(__file__).parent, 'data_orig/fahrgast.xlsx')
                           , sheet_name=None , engine='openpyxl',  header=None)
    dat_sheet_names = []
    for key in sheets:
        if key.startswith('Zeitreihe'):
             dat_sheet_names.append(key)
    dat_sheets = []
    for sheet_name in dat_sheet_names:
        zeitreihe_x = pd.read_excel(os.path.join(pathlib.Path(__file__).parent, 'data_orig/fahrgast.xlsx'),
                                    sheet_name=sheet_name , engine='openpyxl', header=None).T
        new_header = zeitreihe_x.iloc[0]
        zeitreihe_x=zeitreihe_x[1:]
        zeitreihe_x.columns=new_header
        zeitreihe_x["Date"]=sheet_name.strip('Zeitreihe ')
        zeitreihe_x["Kalenderwoche"]=zeitreihe_x["Kalenderwoche"].str.strip('KW')
        def year_week(y,w):
                return datetime.strptime(f'{y} {w} 1', '%G %V %u')
        zeitreihe_x["Datum"]=zeitreihe_x.apply(lambda row: year_week(row.Date, row.Kalenderwoche), axis=1)
        dat_sheets.append(zeitreihe_x)
    fahrgast = pd.concat(dat_sheets)#.reset_index(drop=True) #Dateien werden zusammengeführt und Index (erste Spalte) wird korrigiert
    fahrgast = fahrgast [["Datum", "Fahrgäste (Einsteiger)", "Kalenderwoche"]]
    fahrgast = fahrgast.rename(columns={"Datum": 'startdatum_woche'})
    export_filename = os.path.join(pathlib.Path(__file__).parent, 'data/bvb_fahrgastzahlen.csv')
    fahrgast.to_csv(export_filename, index = False)
    if ct.has_changed(export_filename):
        common.upload_ftp(export_filename, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass,
                          'bvb/fahrgastzahlen')
        odsp.publish_ods_dataset_by_id('100075')
        ct.update_hash_file(export_filename)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')