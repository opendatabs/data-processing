import pytest
# import pandas as pd
import io
import common
from ethz_covid_reprod.credentials import credentials
from ethz_covid_reprod.src import verdoppelung


@pytest.fixture
def initial_file():
    url = 'https://raw.githubusercontent.com/covid-19-Re/dailyRe-Data/master/CHE-estimates.csv'
    req = common.requests_get(url, proxies=credentials.proxies)
    s = req.content
    initial_df = common.pandas_read_csv(io.StringIO(s.decode('utf-8')))
    return initial_df


class TestAddVerdoppelung(object):
    def test_if_no_changes_to_initial_file(self, initial_file):
        expected = initial_file.copy()
        # remove the 6 columns that are added by add_verdoppelung()
        actual = verdoppelung.add_verdoppelung(initial_file).iloc[:, :-6]
        assert actual.equals(expected)
