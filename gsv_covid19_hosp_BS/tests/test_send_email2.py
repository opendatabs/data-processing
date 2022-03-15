import pytest
from datetime import date, time
import pandas as pd
# import io
# import common
from gsv_covid19_hosp_BS.send_email2 import check_if_email

time_for_email = time(9,30)
time_for_email_to_call = time(9,50)
time_for_email_final_status = time(10)


class TestCheckIfEmail(object):
    def test_all_is_filled_before_10(self):
        test_argument = pd.read_csv("test files/weekday_all_filled.csv", keep_default_na=False)
        expected_return = pd.read_csv("test files/weekday_all_filled_return.csv", keep_default_na=False)
        actual_return = check_if_email(df_log=test_argument, date=str(date(2022, 1, 28)), day="today", now_in_switzerland=time(9,15),
                                       time_for_email=time_for_email, time_for_email_to_call=time_for_email_to_call, time_for_email_final_status=time_for_email_final_status)
        pd.testing.assert_frame_equal(expected_return, actual_return)

    def test_missing_after_time_for_email(self):
        test_argument = pd.read_csv("test files/weekday_missing_entry.csv", keep_default_na=False)
        expected_return = pd.read_csv("test files/weekday_missing_entry_after_time_for_email_return.csv", keep_default_na=False)
        actual_return = check_if_email(df_log=test_argument, date=str(date(2022, 1, 28)), day="today", now_in_switzerland=time(9,35),
                                       time_for_email=time_for_email, time_for_email_to_call=time_for_email_to_call, time_for_email_final_status=time_for_email_final_status)
        pd.testing.assert_frame_equal(expected_return, actual_return)

    def test_missing_after_time_to_call(self):
        test_argument = pd.read_csv("test files/weekday_missing_entry.csv", keep_default_na=False)
        expected_return = pd.read_csv("test files/weekday_missing_entry_after_time_to_call_return.csv", keep_default_na=False)
        actual_return = check_if_email(df_log=test_argument, date=str(date(2022, 1, 28)), day="today", now_in_switzerland=time(9,51),
                                       time_for_email=time_for_email, time_for_email_to_call=time_for_email_to_call, time_for_email_final_status=time_for_email_final_status)
        pd.testing.assert_frame_equal(expected_return, actual_return)

    def test_all_filled_after_10(self):
        test_argument = pd.read_csv("test files/weekday_all_filled.csv", keep_default_na=False)
        expected_return = pd.read_csv("test files/weekday_all_filled_return_after_10.csv", keep_default_na=False)
        actual_return = check_if_email(df_log=test_argument, date=str(date(2022, 1, 28)), day="today", now_in_switzerland=time(10,10),
                                       time_for_email=time_for_email, time_for_email_to_call=time_for_email_to_call, time_for_email_final_status=time_for_email_final_status)
        pd.testing.assert_frame_equal(expected_return, actual_return)

    def test_not_all_filled_after_10(self):
        test_argument = pd.read_csv("test files/weekday_missing_entry.csv", keep_default_na=False)
        expected_return = pd.read_csv("test files/weekday_missing_entry_after_10_return.csv",  keep_default_na=False)
        actual_return = check_if_email(df_log=test_argument, date=str(date(2022, 1, 28)), day="today",  now_in_switzerland=time(10,15),
                                       time_for_email=time_for_email, time_for_email_to_call=time_for_email_to_call, time_for_email_final_status=time_for_email_final_status)
        pd.testing.assert_frame_equal(expected_return, actual_return)

    def test_missing_saturday(self):
        test_argument = pd.read_csv("test files/monday_missing_weekend_entry.csv", keep_default_na=False)
        expected_return = pd.read_csv("test files/monday_missing_weekend_entry_return.csv", keep_default_na=False)
        actual_return = check_if_email(df_log=test_argument, date=str(date(2022, 1, 22)), day="Saturday", now_in_switzerland=time(9,10),
                                       time_for_email=time_for_email, time_for_email_to_call=time_for_email_to_call, time_for_email_final_status=time_for_email_final_status)
        pd.testing.assert_frame_equal(expected_return, actual_return)

    def test_missing_sunday(self):
        test_argument = pd.read_csv("test files/monday_missing_weekend_entry_2.csv", keep_default_na=False)
        expected_return = pd.read_csv("test files/monday_missing_weekend_entry_2_return.csv", keep_default_na=False)
        actual_return = check_if_email(df_log=test_argument, date=str(date(2022, 1, 30)), day="Sunday", now_in_switzerland=time(9,10),
                                       time_for_email=time_for_email, time_for_email_to_call=time_for_email_to_call, time_for_email_final_status=time_for_email_final_status)
        pd.testing.assert_frame_equal(expected_return, actual_return)

    def test_missing_after_time_for_email_weekend(self):
        test_argument = pd.read_csv("test files/monday_missing_entry.csv", keep_default_na=False)
        expected_return = pd.read_csv("test files/monday_missing_entry_after_time_for_email_return.csv",
                                      keep_default_na=False)
        actual_return = check_if_email(df_log=test_argument, date=str(date(2022, 1, 24)), day="today", now_in_switzerland=time(9,35),
                                       time_for_email=time_for_email, time_for_email_to_call=time_for_email_to_call, time_for_email_final_status=time_for_email_final_status)
        pd.testing.assert_frame_equal(expected_return, actual_return)

    def test_all_is_filled_before_10_weekend(self):
        test_argument = pd.read_csv("test files/monday_all_filled.csv", keep_default_na=False)
        expected_return = pd.read_csv("test files/monday_all_filled_return.csv", keep_default_na=False)
        actual_return = check_if_email(df_log=test_argument, date=str(date(2022, 1, 24)), day="today", now_in_switzerland=time(9,15),
                                       time_for_email=time_for_email, time_for_email_to_call=time_for_email_to_call, time_for_email_final_status=time_for_email_final_status)
        pd.testing.assert_frame_equal(expected_return, actual_return)

class TestSendEmail(object):
    def test_obtained_negative_value(self):
        pass
    pass
