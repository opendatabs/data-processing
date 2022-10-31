import common
from staka_briefliche_stimmabgaben import credentials
import json
import logging

data_missing_from = 0
def main(data_missing_from=data_missing_from):
    data_test = data_page(date_str=date_str, data_missing_from=data_missing_from)
    common.requests_put('https://data.bs.ch/api/management/v2/pages/test-hester/',
                        data=data_test,
                        auth=(credentials.username, credentials.password)
                        )


def get_slug():
    req = common.requests_get('https://data.bs.ch/api/management/v2/pages/test-hester/',
                              auth=(credentials.username, credentials.password)
                              )
    slug = req.json()
    return slug


date_str = "25 September 2022"
slug = get_slug()
html_slug = slug["content"]["html"]["de"]
css_slug = slug["content"]["css"]["de"]


def page_html():
    html_str = json.dumps(html_slug)
    return html_str


def page_css():
    css_str = json.dumps(css_slug)
    return css_str

def update_css(data_missing_from=data_missing_from):
    if data_missing_from == -1:
        text = ""
    else:
        days = [18, 11, 6, 5, 4, 3, 2, 1, 0]
        days_missing = [i for i in days if i <= data_missing_from]
        days_to_remove = len(days_missing)
        text = ""
        for i in range(days_to_remove):
            number = 9 - i
            add_text = f".pick-values-20220925 .highcharts-data-label:nth-child({number}){{\n    display: none;\n}}"
            text = text + add_text
    css_str = json.dumps(text)
    return css_str

def data_page(date_str, data_missing_from=data_missing_from):
    html_code = page_html()
    css_code = update_css(data_missing_from=data_missing_from)
    title = f' "Briefliche Stimmbeteiligung {date_str}" '
    return (f'{{"title": {{"de":{title}, \n'
            f'"fr": {title},  \n'
            f'"en": {title} \n'
            f'}}, \n'
            f'"description": "", \n'
            f'"template": "contact.html", \n'
            f'"content": \n'
            f'{{"html": {{"de":{html_code},\n'
            f'"fr": {html_code}, \n'
            f' "en": {html_code}}}, \n'
            f'"css": {{"de": {css_code},\n'
            f'"fr": {css_code}, \n'
            f'"en": {css_code}}}}}, \n'
            f'"tags": [],\n'
            f'"restricted": true}}'
            )


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main(data_missing_from=11)

