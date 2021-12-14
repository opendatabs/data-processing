# Note: ECMO is only for USB
import pandas as pd


def add_discrepancy_in_ICU_beds_without_vent(ies_numbers):
    df = ies_numbers
    Total_IS_Pat = df['TotalIcuPats']
    Beatmete_IS_Pat = df['VentIcuPats']
    Betriebene_IS_Betten = df['OperIcuBeds']
    Beatmete_IS_Betten = df['VentIcuBeds']
    Betriebene_IS_Betten_ohne_Beatmung = Betriebene_IS_Betten - Beatmete_IS_Betten
    df['C'] = Betriebene_IS_Betten_ohne_Beatmung - Total_IS_Pat + Beatmete_IS_Pat
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
    df_coreport[["Hospital", "NoauResid", "CapacDate", "CapacTime"]] = \
        df[["Hospital", "NoauResid", "CapacDate", "CapacTime"]]

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
    Betriebene_ECMO_Betten = 5  # not available (for USB) at the moment, we take ECMO = 5...
    Total_ECMO_Pat = df['EcmoPats']


    # numbers to return
    df_coreport['Bettenanzahl frei "Normalstation" COVID'] = Total_Betten_COVID - Betriebene_IS_Betten_COVID - Betriebene_IMCU_Betten_COVID - Total_Pat_COVID + Total_IS_Pat_COVID + Total_IMCU_Pat_COVID
    df_coreport['Bettenanzahl frei "Normalstation"'] = Total_Betten - Betriebene_IS_Betten - Betriebene_IMCU_Betten - Total_Pat + Total_IS_Pat + Total_IMCU_Pat - df_coreport['Bettenanzahl frei "Normalstation" COVID']
    df_coreport['Bettenanzahl frei "IMCU"'] = Betriebene_IMCU_Betten - Total_IMCU_Pat
    df_coreport['Bettenanzahl frei "IPS ohne Beatmung"'] = df.apply(Betten_frei_IPS_ohne_Beatmung, axis=1)
    df_coreport['Bettenanzahl frei "IPS mit Beatmung"'] = df.apply(Betten_frei_IPS_mit_Beatmung, axis=1)
    df_coreport['Bettenanzahl frei " IPS ECMO"'] = Betriebene_ECMO_Betten - Total_ECMO_Pat
    df_coreport['Bettenanzahl belegt "Normalstation"'] = Total_Pat - Total_IS_Pat - Total_IMCU_Pat
    df_coreport['Bettenanzahl belegt "IMCU"'] = Total_IMCU_Pat
    df_coreport['Bettenanzahl belegt "IPS ohne Beatmung"'] = Total_IS_Pat - Beatmete_IS_Pat
    df_coreport['Bettenanzahl belegt "IPS mit Beatmung"'] = Beatmete_IS_Pat
    df_coreport['Bettenanzahl belegt "IPS ECMO"'] = Total_ECMO_Pat

    return df_coreport
