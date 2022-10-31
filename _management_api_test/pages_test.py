import common
from _management_api_test import credentials
import logging
import json


logging.basicConfig(level=logging.DEBUG)


def get_slug():
    req = common.requests_get('https://data.bs.ch/api/management/v2/pages/test-hester/',
                              auth=(credentials.username, credentials.password)
                              )
    file = req.json()
    return file


date_str = "25 September 2022"
file = get_slug()
html_slug = file["content"]["html"]["de"]
css_slug = file["content"]["css"]["de"]


def page_html():
    html_str = json.dumps(html_slug)
    return html_str


def page_css():
    css_str = json.dumps(css_slug)
    return css_str

def data_page(date_str):
    html_code = page_html()
    css_code = page_css()
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


data_test = data_page(date_str=date_str)

common.requests_put('https://data.bs.ch/api/management/v2/pages/test-hester/',
                    data=data_test,
                    auth=(credentials.username, credentials.password)
                    )

