import pandas as pd 
from datetime import datetime
import logging
import locale


def main():
     BVB_Fahrgast()

def BVB_Fahrgast():
      # für deutsche Sprache 
      locale.setlocale(locale.LC_TIME, "de_DE")

      # letztes Update von BVB merken
      df_orig = pd.read_excel('data_orig/BVB-Fahrgast_240409.xlsx',sheet_name='Monatswerte')
      S_1 = df_orig.iloc[-1].dropna()
      BVB_update= datetime.strptime(S_1.index[-1],'%B').month


      # OGD Update
      df_pro = pd.read_excel('data/BVB_to-upload.xlsx')
      last_update= df_pro.iloc[-1]['Startdatum Kalenderwoche/Monat'].month
     
      if last_update > BVB_update:
          print('Error(wrong file)')
      elif last_update == BVB_update:
          pass
      else:
        month_begin = datetime.now().strftime('%Y-'+ str(BVB_update).zfill(2) +'-01 00:00:00')
        df_pro.loc[len(df_pro)] = ['Monat',month_begin, S_1.iloc[-1], None, month_begin]
        df_pro.to_excel('data/export/100075_BVB_Fahrgastzahlen.xlsx', index=False)
      return


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')