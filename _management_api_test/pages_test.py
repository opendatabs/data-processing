import common
import pandas as pd
from _management_api_test import credentials
import html
import logging

import codecs
logging.basicConfig(level=logging.DEBUG)

f=codecs.open("button.html", 'r')
print (f.read())

req = common.requests_get('https://data.bs.ch/api/management/v2/pages/test-hester/',
                            auth=(credentials.username, credentials.password)
                          )
file = req.json()
print(file)

date_str = "25 September 2022"

html_de = file["content"]["html"]["de"]

html_de = " <div class=\"container-fluid\"\n      ng-init=\"view = 'bs'\">\n                <div class=\"page-header\">\n                    <h1 class=\"page-title\">\n                        COVID-19 Dashboard\n                    </h1>\n             <div class=\"switch-container\">\n                <label class=\"switch\">\n                    <input ng-model=\"view\"\n                           ng-true-value=\"'schweiz'\"\n                           ng-false-value=\"'bs'\"\n                           ng-checked=\"view == 'schweiz'\"\n                           class=\"switch-input\"\n                           type=\"checkbox\">\n                    <div class=\"switch-button\">\n                        <span class=\"switch-button-left\">BS</span>\n                        <span class=\"switch-button-right\">Schweiz</span>\n                    </div>\n                </label>\n            </div>\n            </div>\n</div>\n"

#html_de = "<p>" + html_de.replace('\n', '<br>') + "<p>"

html_de = html.escape(html_de)

def page_html():
    #myctx_parameters = "{'sort':'datum','refine.abstimmungsdatum':['2022/09/25']}"
    myctx_parameters = "test"
    #return('"test"')
    #return('"<ods-dataset-context context=\"myctx\" myctx-dataset=\"100224\" myctx-parameters=\"{\'sort\':\'datum\',\'refine.abstimmungsdatum\':[\'2022/09/25\']}\">\n    <ods-chart class=\"pick-values-20220925\" align-month=\"true\">\n        <ods-chart-query context=\"myctx\" field-x=\"datum\" maxpoints=\"50\">\n            <ods-chart-serie expression-y=\"stimmbeteiligung\" chart-type=\"column\" function-y=\"AVG\" label-y=\"Stimmbeteiligung (%)\" \n                             color=\"#66c2a5\" display-values=\"true\" scientific-display=\"true\" display-units=\"true\"\n                             min=0 max=100 step=20>\n            </ods-chart-serie>\n        </ods-chart-query>\n    </ods-chart>\n\n</ods-dataset-context>"')
    text = '"<ods-dataset-context context=\"myctx\" "'
    html_fr = html.escape(text)
    return (html_fr)

print(page_html())


def page_css():
    # return ( f' "css"')
    return ( ".pick-values-20220925 .highcharts-data-label:nth-child(2){display: none;}")

def data_page(date_str):
    html_code = page_html()
    css_code = page_css()
    title = f' "Briefliche Stimmbeteiligung {date_str}" '
    return (f'{{"title": {{"de":{title}, \n'
            f'"fr": {title},  \n'
                        f'"en": {title} \n'
            f'}}, \n'
             f'"description": "The page description", \n'
             f'"template": "contact.html", \n'
             f'"content": \n'
                 f'{{"html": {{"de":"page content",\n'
                           f'"fr": "Page content", \n'
                        f' "en": "Page content"}}, \n'
                  f' "css": {{"de": "p {{ color: black; }}",\n'
                            f'"fr": "p {{ color: black; }}", \n'
                            f'"en": "p {{ color: black; }}"}}}}, \n'
            f'"tags": ["tag1", "tag2"],\n'
    f'"restricted": true}}'
 )

# test how to change page
#data_test = '{"title": {"fr": "Nouveau titre de la page", ' \
#                      '"en": "New page title"}, "description": "The page description", "template": "contact.html", "content": {"html": {"fr": "Contenu de la page", "en": "Page content"}, "css": {"fr": "p { color: black; }", "en": "p { color: black; }"}}, "tags": ["tag1", "tag2"], "restricted": true}'

data_test = data_page(date_str=date_str)

common.requests_put('https://data.bs.ch/api/management/v2/pages/test-hester/',
                   data=data_test,
                    auth=(credentials.username, credentials.password)
                    )


