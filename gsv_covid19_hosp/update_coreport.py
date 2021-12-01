import logging
import common
import credentials
import calculation

def main(value_id, value):
    payload = {
        "value": value,
        "comment": "Entered by bot"
}

    #value_id = '422640'

    username = credentials.username_coreport
    password = credentials.password_coreport

    r = common.requests_patch(f'https://bl.coreport.ch/de/reports/api/submit/{value_id}', json=payload,
                              auth=(username, password))
    r.raise_for_status()

def get_properties_list(hospital):
    if hospital == 'USB':
        properties_list = ['Betten_frei_Normal', 'Betten_frei_Normal_COVID', 'Betten_frei_IMCU',
                           'Betten_frei_IPS_ohne_Beatmung', 'Betten_frei_IPS_mit_Beatmung',
                           'Betten_frei_ECMO', 'Betten_belegt_Normal', 'Betten_belegt_IMCU',
                          'Betten_belegt_IPS_ohne_Beatmung', 'Betten_belegt_IPS_mit_Beatmung', 'Betten_belegt_ECMO']

    else:
        properties_list = ['Betten_frei_Normal', 'Betten_frei_Normal_COVID', 'Betten_frei_IMCU',
                           'Betten_frei_IPS_ohne_Beatmung', 'Betten_frei_IPS_mit_Beatmung',
                           'Betten_belegt_Normal', 'Betten_belegt_IMCU',
                          'Betten_belegt_IPS_ohne_Beatmung', 'Betten_belegt_IPS_mit_Beatmung']
    return properties_list


def write_in_coreport(df, hospital_list):
    df_coreport = calculation.calculate_numbers(df)
    print(df_coreport)
    for hospital in hospital_list:
        df_hospital = df_coreport[df_coreport["Hospital"] == hospital]
        properties = get_properties_list(hospital=hospital)
        for prop in properties:
            value_id = credentials.dict_coreport[hospital][prop]
            value = df_hospital[prop][0]
            print(value_id, value)



if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main(value_id='422640', value=44)





