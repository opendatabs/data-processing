import logging
import common
import credentials
import calculation
import coreport_scraper

def main(value_id, value):

    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    payload = {
        "value": value,
        "comment": "Entered by bot"
}

    #value_id = '422640'

    username = credentials.username_coreport
    password = credentials.password_coreport

    url = credentials.url_coreport + str(value_id)
    print(url)

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
    df_coreport = calculation.calculate_numbers(df)
    df_coreport =coreport_scraper.add_value_id(df_coreport, date=date)
    print(df_coreport)
    for hospital in hospital_list:
        df_hospital = df_coreport.filter(items=[hospital], axis=0)
        properties = get_properties_list(hospital=hospital)
        for prop in properties:
            #value_id = credentials.dict_coreport[hospital][prop]
            value = int(df_hospital[prop][0])
            value_id = df_hospital[prop + " value_id"][0]
            print(value_id, value)
            main(value_id=value_id, value=value)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    # fill Clara 7.12.2021, betten frei normal
    value_id = coreport_scraper.get_value_id('Clara', '07.12.2021', 'Bettenanzahl frei "Normalstation"')
    main(value_id=value_id, value=39)
    # fill Clara 6.12.2021, betten frei normal
    # main(value_id='427624', value=37)





