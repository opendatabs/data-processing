import io
import logging
from covid19dashboard import credentials
import pandas as pd
import os
import common
import common.change_tracking as ct
import ods_publish.etl_id as odsp


def main():
    sourcefile = 'https://raw.githubusercontent.com/openZH/covid_19/master/COVID19_Fallzahlen_CH_total_v2.csv'
    logging.info(f'Reading data from {sourcefile}...')
    r = common.requests_get(sourcefile)
    r.raise_for_status()
    s = r.text
    df = pd.read_csv(io.StringIO(s))
    logging.info('Getting rid of unnecessary columns...')
    df.drop(columns=['time', 'source', 'ncumul_tested', 'new_hosp', 'current_vent'], inplace=True)

    logging.info('Calculating date range...')
    df.date = pd.to_datetime(df.date)
    date_range = pd.date_range(start=df.date.min(), end=df.date.max())

    # https://skipperkongen.dk/2018/11/26/how-to-fill-missing-dates-in-pandas/
    # https://stackoverflow.com/questions/52430892/forward-filling-missing-dates-into-python-pandas-dataframe
    logging.info('Iterating over each canton, sorting, adding missing dates, then filling the value gaps using ffill()...')
    cantons = df.abbreviation_canton_and_fl.unique()
    df_filled = pd.DataFrame(columns=df.columns)
    for canton in cantons:
        logging.info(f'Working through canton {canton}...')
        df_canton = df[df.abbreviation_canton_and_fl == canton].sort_values(by='date')
        df_canton_filled = df_canton.set_index('date').reindex(date_range).ffill().reset_index().rename(columns={'index': 'date'})

        logging.info('Getting rid of rows with empty date...')
        df_canton_filled.dropna(subset=['abbreviation_canton_and_fl'], inplace=True)

        logging.info('Calculating differences between rows in new columns...')
        df_canton_diff = df_canton_filled.drop(columns=['abbreviation_canton_and_fl']).diff()
        df_canton_filled['ndiff_conf'] = df_canton_diff.ncumul_conf
        df_canton_filled['ndiff_released'] = df_canton_diff.ncumul_released
        df_canton_filled['ndiff_deceased'] = df_canton_diff.ncumul_deceased

        df_filled = df_filled.append(df_canton_filled, ignore_index=True)

    filename = os.path.join(credentials.path, credentials.filename)
    logging.info(f'Exporting data to {filename}')
    df_filled.to_csv(filename, index=False)
    if ct.has_changed(filename):
        common.upload_ftp(filename, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'covid19dashboard')
        odsp.publish_ods_dataset_by_id('100085')
        ct.update_hash_file(filename)
    logging.info('Job successful!')


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()

# If proxy will ever block url raw.githubusercontent.com again: use the code below to download file contents via github REST API: 
# # see https://github.com/PyGithub/PyGithub/issues/661
# import base64
# from github import Github
# 
# 
# def get_blob_content(repo, branch, path_name):
#     ref = repo.get_git_ref(f'heads/{branch}')
#     tree = repo.get_git_tree(ref.object.sha, recursive='/' in path_name).tree
#     sha = [x.sha for x in tree if x.path == path_name]
#     if not sha:
#         raise FileNotFoundError(path_name)
#     # return repo.get_git_blob(sha[0])
#     blob = repo.get_git_blob(sha[0])
#     b64 = base64.b64decode(blob.content)
#     return b64.decode("utf8")
# 
# 
# def retrieve_file_from_github():
#     g = Github()
#     repo = g.get_repo(full_name_or_id='openzh/covid_19')
#     file_contents = get_blob_content(repo=repo, branch='master', path_name='COVID19_Fallzahlen_CH_total_v2.csv')
# 
