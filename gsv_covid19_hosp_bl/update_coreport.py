
from datetime import timezone, datetime
import numpy as np
import logging
import common
from gsv_covid19_hosp_bl import credentials
from gsv_covid19_hosp_bl import calculation
from gsv_covid19_hosp_bl import send_email2
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
    if hospital == 'Arlesheim':
        properties_list = ['Bettenanzahl frei "Normal"', 'Bettenanzahl frei "IMCU"',
                           'Bettenanzahl belegt "Normal"','Bettenanzahl belegt "IMCU"',
                           'Anzahl Patienten Normal COVID', 'Anzahl Patienten IMCU COVID mit Beatmung',
                           'Anzahl Patienten IMCU COVID ohne Beatmung']

    else:
        properties_list = ['Bettenanzahl frei "Normal"', 'Bettenanzahl frei "IPS ohne Beatmung"',
                           'Bettenanzahl frei "IPS mit Beatmung"', 'Bettenanzahl belegt "Normal" inkl. COVID Verdachtsfälle',
                           'Bettenanzahl belegt "Normal" COVID', 'Bettenanzahl belegt "IPS ohne Beatmung"',
                           'Bettenanzahl belegt "IPS mit Beatmung"', 'Anzahl Patienten "IPS nicht Beatmet" inkl. COVID Verdachtsfälle',
                           'Anzahl Patienten "IPS  Beatmet"  inkl. COVID Verdachtsfälle', 'Anzahl Patienten "IPS nicht Beatmet" COVID',
                           'Anzahl Patienten "IPS  Beatmet" COVID']
    return properties_list


def add_value_id(df, date):
    df = df.astype(str)
    url_api = credentials.url_coreport_api
    username = credentials.username_coreport
    password = credentials.password_coreport
    timeslot = date.strftime('%d-%m-%Y')
    columns = list(df.columns[4:])
    dict_org = credentials.dict_organization
    for data_name in columns:
        df[data_name + " value_id"] = ""
    hospitals = list(df["Hospital"])
    df.set_index("Hospital", inplace=True)
    for hospital in hospitals:
        organization = dict_org[hospital]
        data_names = get_properties_list(hospital=hospital)
        for data_name in data_names:
            filter_result = f'&organization={organization}&timeslot={timeslot}&question={data_name}'
            url = url_api + filter_result
            req = common.requests_get(url, auth=(username, password))
            print(data_name)
            print(req.json())
            result = req.json()[0]
            # make sure first result indeed has the right date
            assert result['timeslot']['deadline'] == timeslot
            value_id = result['id']
            df.loc[hospital, data_name + " value_id"] = str(value_id)
    return df


def write_in_coreport(df, hospital_list, date, day, df_log,
                      current_time=datetime.now(timezone.utc)
                      .astimezone(ZoneInfo('Europe/Zurich')).time().replace(microsecond=0)):
    logging.info("Calculate numbers for CoReport")
    df_coreport = calculation.calculate_numbers(df)
    logging.info("Get value id's from CoReport")
    df_coreport = add_value_id(df_coreport, date=date)
    for hospital in hospital_list:
        logging.info(f"Write entries into CoReport for {hospital}")
        df_hospital = df_coreport.filter(items=[hospital], axis=0)
        properties = get_properties_list(hospital=hospital)
        # index_hospital = df_coreport.index[df_coreport["Hospital"] == hospital]
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
                logging.warning(f"Negative value for {prop} of {hospital}!")
                condition = (df_log["Date"] == date) & (df_log["Hospital"] == hospital)
                incomplete += 1
                if (df_log.loc[condition, 'email_negative_value'] == "").all():
                    logging.info(f"send email about negative value for {prop} of {hospital}")
                    send_email2.send_email(hospital=hospital, email_type="Negative value", day=day,
                                           extra_info=[prop, hospital])
                    df_log.loc[condition, 'email_negative_value'] = f"Sent at {current_time}"
                else:
                    email_send_at = df_log.loc[condition, 'email_negative_value']
                    logging.info(f"email about negative value for {prop} of {hospital} has been sent: {email_send_at}")
        condition = (df_log["Date"] == date) & (df_log["Hospital"] == hospital)
        if incomplete == 0:
            df_log.loc[condition, 'CoReport_filled'] = "Yes"
            logging.info(f"Entries added into CoReport for {hospital}")
        else:
            df_log.loc[condition, 'CoReport_filled'] = "Not all filled"
            logging.warning(f"Entries only partly added into CoReport for {hospital}")
    return df_log
