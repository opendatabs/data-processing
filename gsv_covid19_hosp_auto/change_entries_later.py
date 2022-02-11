import pandas as pd
import update_coreport


def get_df_manually(hospital, date, TotalAllBeds, TotalAllBedsC19, OperIcuBeds,
                 OperIcuBedsC19, VentIcuBeds, OperImcBeds, OperImcBedsC19, TotalAllPats, TotalAllPatsC19,
                 TotalIcuPats, TotalIcuPatsC19, VentIcuPats, TotalImcPats, TotalImcPatsC19, EcmoPats):

    row = [[hospital, date, TotalAllBeds, TotalAllBedsC19, OperIcuBeds,
                 OperIcuBedsC19, VentIcuBeds, OperImcBeds, OperImcBedsC19, TotalAllPats, TotalAllPatsC19,
                 TotalIcuPats, TotalIcuPatsC19, VentIcuPats, TotalImcPats, TotalImcPatsC19, EcmoPats]]
    df = pd.DataFrame(row, columns=['Hospital', 'Date', 'TotalAllBeds', 'TotalAllBedsC19', 'OperIcuBeds', 'OperIcuBedsC19',
                'VentIcuBeds', 'OperImcBeds', 'OperImcBedsC19', 'TotalAllPats', 'TotalAllPatsC19', 'TotalIcuPats',
                'TotalIcuPatsC19', 'VentIcuPats', 'TotalImcPats', 'TotalImcPatsC19', 'EcmoPats'])

    return df


def write_manually(hospital, date, TotalAllBeds, TotalAllBedsC19, OperIcuBeds,
                 OperIcuBedsC19, VentIcuBeds, OperImcBeds, OperImcBedsC19, TotalAllPats, TotalAllPatsC19,
                 TotalIcuPats, TotalIcuPatsC19, VentIcuPats, TotalImcPats, TotalImcPatsC19, EcmoPats ):
    df = get_df_manually(hospital, date, TotalAllBeds, TotalAllBedsC19, OperIcuBeds,
                 OperIcuBedsC19, VentIcuBeds, OperImcBeds, OperImcBedsC19, TotalAllPats, TotalAllPatsC19,
                 TotalIcuPats, TotalIcuPatsC19, VentIcuPats, TotalImcPats, TotalImcPatsC19, EcmoPats)
    # need still to make sure date format is correct
    update_coreport.write_in_coreport(df=df, hospital_list=[hospital], date=date)