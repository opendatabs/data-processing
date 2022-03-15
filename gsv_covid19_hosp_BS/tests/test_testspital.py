from datetime import date, time
import pandas as pd
# import io
# import common
from gsv_covid19_hosp_BS.tests.testspital import write_in_coreport_test



class TestWriteInCoreportTest(object):
    """
    To do:
    check what happens with negative values, should enter "Not all filled" in log file, only for the hospital entry it concerns..
    need: df with data-entries that produce a negative value
    """
    def test_negative_value(self):
        df_input = pd.read_csv("test files/df_with_negative_value.csv")
        day="Today"
        df_log = pd.read_csv("test files/weekday_all_entered_IES.csv", keep_default_na=False)
        current_time = time(9,15)
        expected_return = pd.read_csv("test files/weekday_negative_value_log_returned.csv", keep_default_na=False)
        actual_return = write_in_coreport_test(df=df_input, hospital_list=['Clara', 'UKBB', 'USB'], date=date(2022,1,27), day=day, df_log=df_log, current_time=current_time )
        actual_return.to_csv("test files/return_neg_value.csv", index=False)
        actual_return = pd.read_csv("test files/return_neg_value.csv", keep_default_na=False)
        pd.testing.assert_frame_equal(expected_return, actual_return)


    def test_no_negative_value(self):
        pass
