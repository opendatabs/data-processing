"""
check which day it is, differentiate between monday and other weekdays, variables: day

get data of the day at 9:15, and the weekend if it's Monday, variables: day, data

if no data there, send email, check again after 15 minutes and get data, if still no data, give warning

make dataframe with data of the day (+weekend), variables in:day, data ; variables out: day, data frame

Select latest entry of each day, variables in: day, dataframe, variables out: day, data row

Calculate needed numbers: variabels in: day, data row ; variables out: day, new data row

[Betten_frei_Normal, Betten_frei_Normal_COVID, Betten_frei_IMCU, Betten_frei_IPS_ohne_Beatmung,
      Betten_frei_IPS_mit_Beatmung, Betten_frei_ECMO, Betten_belegt_Normal, Betten_belegt_IMCU, Betten_belegt_IPS_ohne_Beatmung,
      Betten_belegt_IPS_mit_Beatmung, Betten_belegt_ECMO] = calculate_numbers(ies_numbers)

enter numbers in CoReport
"""
import pandas as pd
import get_data
#import send_email
import calculation
import datetime
import threading
import credentials
import update_coreport


def retry(date, list_hospitals):
    print("retrying")
    df, still_missing_hospitals = get_df_for_date(date=date, list_hospitals=list_hospitals, weekend=False)
    if df.empty == False:
        update_coreport.write_in_coreport(df, hospital_list=list_hospitals, date=date)
        filled_hospitals = [x for x in list_hospitals if x not in still_missing_hospitals]
        print("entries in coreport for ", filled_hospitals)
    if still_missing_hospitals is not []:
        print("Still missing: ", still_missing_hospitals)


def all_together(date, list_hospitals):
    if get_data.check_day() == "Monday":
        saturday = date-datetime.timedelta(2)
        df_saturday, missing_saturday = get_df_for_date(date=saturday, list_hospitals=list_hospitals, weekend=True)
        list_hospitals_sat = [x for x in list_hospitals if x not in missing_saturday]
        update_coreport.write_in_coreport(df_saturday, list_hospitals_sat, date=saturday)
        print("Missing Saturday: ", missing_saturday)
        sunday = date - datetime.timedelta(1)
        df_sunday, missing_sunday = get_df_for_date(date=sunday, list_hospitals=list_hospitals, weekend=True)
        list_hospitals_sun = [x for x in list_hospitals if x not in missing_sunday]
        update_coreport.write_in_coreport(df_sunday, list_hospitals_sun, date=sunday)
        print("Missing Sunday: ", missing_sunday)
        df_monday, missing_hospitals = get_df_for_date(date=date, list_hospitals=list_hospitals, weekend=False)
        filled_hospitals = [x for x in list_hospitals if x not in missing_hospitals]
        update_coreport.write_in_coreport(df_monday, filled_hospitals, date=date)
        if not not missing_hospitals:
            print("repeat after 15 minutes for ", missing_hospitals)
            threading.Timer(900, function=retry, args=[date, missing_hospitals]).start()
    elif get_data.check_day() == "Other workday":
        df, missing_hospitals = get_df_for_date(date=date, list_hospitals=list_hospitals, weekend=False)
        if df.empty == False:
            filled_hospitals = [x for x in list_hospitals if x not in missing_hospitals]
            update_coreport.write_in_coreport(df, filled_hospitals, date=date)
            print("entries in coreport for ", filled_hospitals)
        elif df.empty == True:
            print("dataframe is empty, nothing is entered into coreport")
        if not not missing_hospitals:
            print("repeat after 15 minutes for ", missing_hospitals)
            threading.Timer(900, function=retry, args=[date, missing_hospitals]).start()
    else:
        print("It is weekend")


def get_df_for_date(date, list_hospitals, weekend=False):
    df = pd.DataFrame()
    missing_hospitals = []
    for hospital in list_hospitals:
        result = get_df_for_date_hospital(hospital=hospital, date=date, weekend=weekend)
        if result.empty:
            missing_hospitals.append(hospital)
        else:
            result['Hospital'] = hospital
            df = pd.concat([df, result])
    return df, missing_hospitals


def get_df_for_date_hospital(hospital, date, weekend=False):
    df_entries = get_data.get_dataframe(hospital=hospital, date=date)
    number_of_entries = df_entries.shape[0]
    if number_of_entries == 0:
        if weekend:
            print("Numbers for the weekend day " + str(date) + " are not available for " + hospital +"!")
            return pd.DataFrame()
        else:
            print("send reminder email for " + hospital)
            return pd.DataFrame()
    elif number_of_entries >= 1:
        df_entry = df_entries[df_entries.CapacTime == df_entries.CapacTime.max()]
        return df_entry


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


if __name__ == "__main__":
    pd.set_option('display.max_columns', None)
    date = datetime.datetime.today().date()
    #list_hospitals = ['USB', 'Clara', 'UKBB']
    #list_hospitals = ['UKBB']
    list_hospitals = ['USB', 'Clara']
    all_together(date=date, list_hospitals=list_hospitals)


    """
    df = get_df_manually(hospital='Clara', date='13.12.2021', TotalAllBeds=213, TotalAllBedsC19=24, OperIcuBeds=8,
                 OperIcuBedsC19=4, VentIcuBeds=8, OperImcBeds=4, OperImcBedsC19=0, TotalAllPats=162, TotalAllPatsC19=17,
                 TotalIcuPats=5, TotalIcuPatsC19=3, VentIcuPats=4, TotalImcPats=0, TotalImcPatsC19=0, EcmoPats=0)
    """
