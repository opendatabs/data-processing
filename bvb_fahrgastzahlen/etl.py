import pandas as pd
import openpyxl
from datetime import datetime
import logging
import os
import pathlib


def main():
    sheets = pd.read_excel(os.path.join(pathlib.Path(__file__), '/data_orig/fahrgast.xlsx')
                           , sheet_name=None , engine='openpyxl',  header=None)
    dat_sheet_names = []
    for key in sheets:
        if key.startswith('Zeitreihe'):
             dat_sheet_names.append(key)
    dat_sheets = []
    for sheet_name in dat_sheet_names:
        zeitreihe_x = pd.read_excel(os.path.join(pathlib.Path(__file__), '/data_orig/fahrgast.xlsx'),
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
    fahrgast.to_csv(os.path.join(pathlib.Path(__file__), '/data/bvb_fahrgastzahlen.csv'), index = False)

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')