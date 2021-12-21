import datetime
import logging
import pandas as pd
import common
from gsv_covid19_hosp import credentials
from gsv_covid19_hosp import calculation
from gsv_covid19_hosp import coreport_scraper
from gsv_covid19_hosp import hospitalzahlen

def main(value_id, value):
    # logging.basicConfig(level=logging.DEBUG)
    # logging.info(f'Executing {__file__}...')
    payload = {
        "value": value,
        "comment": "Entered by bot"
}

    username = credentials.username_coreport
    password = credentials.password_coreport

    url = credentials.url_coreport + str(value_id)
    # print(url)

    r = common.requests_patch(url, json=payload,
                              auth=(username, password))
    r.raise_for_status()


def get_properties_list(hospital):
    if hospital == 'USB':
        properties_list =  ['Bettenanzahl frei "Normalstation"', 'Bettenanzahl frei "Normalstation" COVID',
                      'Bettenanzahl frei "IMCU"', 'Bettenanzahl frei "IPS ohne Beatmung"',
                      'Bettenanzahl frei "IPS mit Beatmung"', 'Bettenanzahl belegt "Normalstation"',
                      'Bettenanzahl belegt "IMCU"', 'Bettenanzahl belegt "IPS ohne Beatmung"',
                      'Bettenanzahl belegt "IPS mit Beatmung"', 'Bettenanzahl frei " IPS ECMO"',
                            'Bettenanzahl belegt "IPS ECMO"']


    else:
        properties_list = ['Bettenanzahl frei "Normalstation"', 'Bettenanzahl frei "Normalstation" COVID',
                          'Bettenanzahl frei "IMCU"', 'Bettenanzahl frei "IPS ohne Beatmung"',
                          'Bettenanzahl frei "IPS mit Beatmung"', 'Bettenanzahl belegt "Normalstation"',
                          'Bettenanzahl belegt "IMCU"', 'Bettenanzahl belegt "IPS ohne Beatmung"',
                          'Bettenanzahl belegt "IPS mit Beatmung"']
    return properties_list


def write_in_coreport(df, hospital_list, date):
    logging.info("Calculate numbers for CoReport")
    df_coreport = calculation.calculate_numbers(df)
    logging.info("Get value id's from CoReport")
    df_coreport =coreport_scraper.add_value_id(df_coreport, date=date)
    """
    # with value id's already saved the day before:
    date = date.strftime('%d.%m.%Y')
    file_name = "value_id_df_" + str(date) + ".pkl"
    df_value_id = pd.read_pickle(file_name)
    df_coreport.set_index("Hospital", inplace=True)
    df_coreport = df_coreport.join(df_value_id)
    """
    for hospital in hospital_list:
        logging.info(f"Write entries into CoReport for {hospital}")
        df_hospital = df_coreport.filter(items=[hospital], axis=0)
        properties = get_properties_list(hospital=hospital)
        for prop in properties:
            #value_id = credentials.dict_coreport[hospital][prop]
            value = int(df_hospital[prop][0])
            value_id = df_hospital[prop + " value_id"][0]
            #print(value_id, value)
            main(value_id=value_id, value=value)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')


    df_value_id = pd.read_pickle('value_id_df_14.12.2021.pkl')
    today = datetime.datetime.today().date()
    list_hospitals = ['USB', 'Clara', 'UKBB']
    df_ies, missing = hospitalzahlen.get_df_for_date(date=today, list_hospitals=list_hospitals)
    df_coreport = calculation.calculate_numbers(df_ies)
    df_coreport.set_index("Hospital", inplace=True)
    print(df_coreport)
    print(df_value_id)
    df_joined = df_coreport.join(df_value_id)
    pd.set_option('display.max_columns', None)
    print(df_joined)




