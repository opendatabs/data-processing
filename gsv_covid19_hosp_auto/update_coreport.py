
from datetime import timezone, datetime
import logging
import pandas as pd
import common
from gsv_covid19_hosp_auto import credentials
from gsv_covid19_hosp_auto import calculation
from gsv_covid19_hosp_auto import coreport_scraper
from gsv_covid19_hosp_auto import hospitalzahlen
from gsv_covid19_hosp_auto import send_email2
from zoneinfo import ZoneInfo

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
    logging.info(f'Submitting value "{value}" to url {url}...')
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


def write_in_coreport(df, hospital_list, date, day, df_log, current_time= datetime.now(timezone.utc).astimezone(ZoneInfo('Europe/Zurich')).time().replace(microsecond=0)):
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
        #index_hospital = df_coreport.index[df_coreport["Hospital"] == hospital]
        logging.info(f"Write entries into CoReport for {hospital}")
        incomplete = 0
        for prop in properties:
            # value_id = credentials.dict_coreport[hospital][prop]
            value = int(df_hospital[prop][0])
            value_id = df_hospital[prop + " value_id"][0]
            # quick fix to ignore negative values
            if value >= 0:
                main(value_id=value_id, value=value)
                logging.info(f"Added {value} for {prop} of {hospital} ")
            else:
                logging.warning(f"Negative value for {prop} of {hospital}! send email...")
                condition = (df_log["Date"] == date) & (df_log["Hospital"] == hospital)
                incomplete += 1
                if (df_log.loc[condition, "email negative value"] == "").all():
                    send_email2.send_email(hospital=hospital, email_type="Negative value", day=day,
                                           extra_info=[prop, hospital])
                    df_log.loc[condition, "email negative value"] = f"Sent at {current_time}"
        condition = (df_log["Date"] == date) & (df_log["Hospital"] == hospital)
        if incomplete == 0:
            df_log.loc[condition, "CoReport filled"] = "Yes"
            logging.info(f"Entries added into CoReport for {hospital}")
        else:
            df_log.loc[condition, "CoReport filled"] = "Not all filled"
            logging.warning(f"Entries only partly added into CoReport for {hospital}")
    return df_log

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




