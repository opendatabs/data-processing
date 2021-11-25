# Note: ECMO is only for USB
import pandas as pd
from example import example
import get_data


ies_numbers = get_data.make_dataframe()


def add_discrepancy_in_ICU_beds_without_vent(ies_numbers):
    df = ies_numbers
    Total_IS_Pat = df['TotalIcuPats']
    Beatmete_IS_Pat = df['VentIcuPats']
    Betriebene_IS_Betten = df['OperIcuBeds']
    Beatmete_IS_Betten = df['VentIcuBeds']
    Betriebene_IS_Betten_ohne_Beatmung = Betriebene_IS_Betten - Beatmete_IS_Betten
    df['C'] = Betriebene_IS_Betten_ohne_Beatmung - Total_IS_Pat + Beatmete_IS_Pat
    print(df['C'])
    return df


def Betten_frei_IPS_ohne_Beatmung(df):
    if df['C'] < 0:
        return 0
    else:
        return df['C']

def Betten_frei_IPS_mit_Beatmung(df):
    Beatmete_IS_Pat = df['VentIcuPats']
    Beatmete_IS_Betten = df['VentIcuBeds']
    if df['C'] < 0:
        return Beatmete_IS_Betten - Beatmete_IS_Pat + df['C']
    else:
        return Beatmete_IS_Betten - Beatmete_IS_Pat


def calculate_numbers(ies_numbers):
    df = add_discrepancy_in_ICU_beds_without_vent(ies_numbers)
    df_coreport = pd.DataFrame()
    df_coreport[["NoauResid", "CapacDate", "CapacTime"]] = df[["NoauResid", "CapacDate", "CapacTime"]]

    Total_Betten = df['TotalAllBeds']
    Total_Betten_COVID = df['TotalAllBedsC19']
    Betriebene_IS_Betten = df['OperIcuBeds']
    Betriebene_IS_Betten_COVID = df['OperIcuBedsC19']
    Beatmete_IS_Betten = df['VentIcuBeds']
    Betriebene_IMCU_Betten = df['OperImcBeds']
    Betriebene_IMCU_Betten_COVID = df['OperImcBedsC19']
    Total_Pat = df['TotalAllPats']
    Total_Pat_COVID = df['TotalAllPatsC19']
    Total_IS_Pat = df['TotalIcuPats']
    Total_IS_Pat_COVID = df['TotalIcuPatsC19']
    Beatmete_IS_Pat = df['VentIcuPats']
    Total_IMCU_Pat = df['TotalImcPats']
    Total_IMCU_Pat_COVID = df['TotalImcPatsC19']

    Betriebene_ECMO_Betten = 5  # not available for USB at the moment, we take ECMO = 5...
    Total_ECMO_Pat = df['EcmoPats']


    # numbers to return
    df_coreport['Betten_frei_Normal_COVID'] = Total_Betten_COVID - Betriebene_IS_Betten_COVID - Betriebene_IMCU_Betten_COVID - Total_Pat_COVID + Total_IS_Pat_COVID + Total_IMCU_Pat_COVID

    df_coreport['Betten_frei_Normal'] = Total_Betten - Betriebene_IS_Betten - Betriebene_IMCU_Betten - Total_Pat + Total_IS_Pat + Total_IMCU_Pat - df_coreport['Betten_frei_Normal_COVID']

    df_coreport['Betten_frei_IMCU'] = Betriebene_IMCU_Betten - Total_IMCU_Pat
    # Betten_frei_IPS_ohne_Beatmung:
    df_coreport['Betten_frei_IPS_ohne_Beatmung'] = df.apply(Betten_frei_IPS_ohne_Beatmung, axis=1)

    df_coreport['Betten_frei_IPS_mit_Beatmung'] = df.apply(Betten_frei_IPS_mit_Beatmung, axis=1)
    # Betten_frei_IPS_mit_Beatmung:
   # if C < 0:
    #    df_coreport['Betten_frei_IPS_mit_Beatmung'] = Beatmete_IS_Betten - Beatmete_IS_Pat + C
    #else:
     #   df_coreport['Betten_frei_IPS_mit_Beatmung'] = Beatmete_IS_Betten - Beatmete_IS_Pat

    df_coreport['Betten_frei_ECMO'] = Betriebene_ECMO_Betten - Total_ECMO_Pat
    df_coreport['Betten_belegt_Normal'] = Total_Pat - Total_IS_Pat - Total_IMCU_Pat
    df_coreport['Betten_belegt_IMCU'] = Total_IMCU_Pat
    df_coreport['Betten_belegt_IPS_ohne_Beatmung'] = Total_IS_Pat - Beatmete_IS_Pat
    df_coreport['Betten_belegt_IPS_mit_Beatmung'] = Beatmete_IS_Pat
    df_coreport['Betten_belegt_ECMO'] = Total_ECMO_Pat

    return df_coreport


df_coreport = calculate_numbers(ies_numbers=ies_numbers)
pd.set_option('display.max_columns', None)
print(df_coreport.head())
print(df_coreport.loc[[0]])

"""
# In between result

Betriebene_IS_Betten_ohne_Beatmung = Betriebene_IS_Betten - Beatmete_IS_Betten
C = Betriebene_IS_Betten_ohne_Beatmung - Total_IS_Pat + Beatmete_IS_Pat

# numbers to return
Betten_frei_Normal_COVID = Total_Betten_COVID- Betriebene_IS_Betten_COVID - Betriebene_IMCU_Betten_COVID - Total_Pat_COVID + Total_IS_Pat_COVID + Total_IMCU_Pat_COVID

Betten_frei_Normal = Total_Betten - Betriebene_IS_Betten - Betriebene_IMCU_Betten - Total_Pat + Total_IS_Pat + Total_IMCU_Pat - Betten_frei_Normal_COVID

Betten_frei_IMCU = Betriebene_IMCU_Betten - Total_IMCU_Pat
# Betten_frei_IPS_ohne_Beatmung:
if C < 0:
    Betten_frei_IPS_ohne_Beatmung = 0
else:
    Betten_frei_IPS_ohne_Beatmung = C

# Betten_frei_IPS_mit_Beatmung:
if C < 0:
    Betten_frei_IPS_mit_Beatmung = Beatmete_IS_Betten - Beatmete_IS_Pat + C
else:
    Betten_frei_IPS_mit_Beatmung = Beatmete_IS_Betten - Beatmete_IS_Pat

Betten_frei_ECMO = Betriebene_ECMO_Betten - Total_ECMO_Pat
Betten_belegt_Normal = Total_Pat - Total_IS_Pat - Total_IMCU_Pat
Betten_belegt_IMCU = Total_IMCU_Pat
Betten_belegt_IPS_ohne_Beatmung = Total_IS_Pat - Beatmete_IS_Pat
Betten_belegt_IPS_mit_Beatmung = Beatmete_IS_Pat
Betten_belegt_ECMO = Total_ECMO_Pat


print(Betten_frei_Normal, Betten_frei_Normal_COVID, Betten_frei_IMCU, Betten_frei_IPS_ohne_Beatmung,
      Betten_frei_IPS_mit_Beatmung, Betten_frei_ECMO, Betten_belegt_Normal, Betten_belegt_IMCU, Betten_belegt_IPS_ohne_Beatmung,
      Betten_belegt_IPS_mit_Beatmung, Betten_belegt_ECMO)



"""

"""
Total_Betten = 801
Total_Betten_COVID = 41
Betriebene_IS_Betten = 42
Betriebene_IS_Betten_COVID = 12
Beatmete_IS_Betten = 30
Betriebene_ECMO_Betten = 5
Betriebene_IMCU_Betten = 20
Betriebene_IMCU_Betten_COVID = 0
Total_Pat = 602
Total_Pat_COVID = 24
Total_IS_Pat = 36
Total_IS_Pat_COVID = 4
Beatmete_IS_Pat = 11
Total_ECMO_Pat = 0
Total_IMCU_Pat = 18
Total_IMCU_Pat_COVID = 0
"""