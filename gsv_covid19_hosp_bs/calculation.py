# Note: ECMO is only for USB
import pandas as pd


def add_discrepancy_in_icu_beds_without_vent(ies_numbers):
    df = ies_numbers
    total_is_pat = df['TotalIcuPats']
    beatmete_is_pat = df['VentIcuPats']
    betriebene_is_betten = df['OperIcuBeds']
    beatmete_is_betten = df['VentIcuBeds']
    betriebene_is_betten_ohne_beatmung = betriebene_is_betten - beatmete_is_betten
    df['C'] = betriebene_is_betten_ohne_beatmung - total_is_pat + beatmete_is_pat
    return df


def betten_frei_ips_ohne_beatmung(df):
    if df['C'] < 0:
        return 0
    else:
        return df['C']


def betten_frei_ips_mit_beatmung(df):
    beatmete_is_pat = df['VentIcuPats']
    beatmete_is_betten = df['VentIcuBeds']
    if df['C'] < 0:
        return beatmete_is_betten - beatmete_is_pat + df['C']
    else:
        return beatmete_is_betten - beatmete_is_pat


def calculate_numbers(ies_numbers):
    df = add_discrepancy_in_icu_beds_without_vent(ies_numbers)
    df_coreport = pd.DataFrame()
    df_coreport[["Hospital", "NoauResid", "CapacDate", "CapacTime"]] = \
        df[["Hospital", "NoauResid", "CapacDate", "CapacTime"]]

    total_betten = df['TotalAllBeds']
    total_betten_covid = df['TotalAllBedsC19']
    betriebene_is_betten = df['OperIcuBeds']
    betriebene_is_betten_covid = df['OperIcuBedsC19']
    # Beatmete_IS_Betten = df['VentIcuBeds']
    betriebene_imcu_betten = df['OperImcBeds']
    betriebene_imcu_betten_covid = df['OperImcBedsC19']
    total_pat = df['TotalAllPats']
    total_pat_covid = df['TotalAllPatsC19']
    total_is_pat = df['TotalIcuPats']
    total_is_pat_covid = df['TotalIcuPatsC19']
    beatmete_is_pat = df['VentIcuPats']
    total_imcu_pat = df['TotalImcPats']
    total_imcu_pat_covid = df['TotalImcPatsC19']
    betriebene_ecmo_betten = 5  # not available (for USB) at the moment, we take ECMO = 5...
    total_ecmo_pat = df['EcmoPats']

    # numbers to return
    df_coreport['Bettenanzahl frei "Normalstation" COVID'] = \
        total_betten_covid - betriebene_is_betten_covid - betriebene_imcu_betten_covid \
        - total_pat_covid + total_is_pat_covid + total_imcu_pat_covid
    df_coreport['Bettenanzahl frei "Normalstation"'] = \
        total_betten - betriebene_is_betten - betriebene_imcu_betten \
        - total_pat + total_is_pat + total_imcu_pat - df_coreport['Bettenanzahl frei "Normalstation" COVID']
    df_coreport['Bettenanzahl frei "IMCU"'] = betriebene_imcu_betten - total_imcu_pat
    df_coreport['Bettenanzahl frei "IPS ohne Beatmung"'] = df.apply(betten_frei_ips_ohne_beatmung, axis=1)
    df_coreport['Bettenanzahl frei "IPS mit Beatmung"'] = df.apply(betten_frei_ips_mit_beatmung, axis=1)
    df_coreport['Bettenanzahl frei " IPS ECMO"'] = betriebene_ecmo_betten - total_ecmo_pat
    df_coreport['Bettenanzahl belegt "Normalstation"'] = total_pat - total_is_pat - total_imcu_pat
    df_coreport['Bettenanzahl belegt "IMCU"'] = total_imcu_pat
    df_coreport['Bettenanzahl belegt "IPS ohne Beatmung"'] = total_is_pat - beatmete_is_pat
    df_coreport['Bettenanzahl belegt "IPS mit Beatmung"'] = beatmete_is_pat
    df_coreport['Bettenanzahl belegt "IPS ECMO"'] = total_ecmo_pat

    return df_coreport
