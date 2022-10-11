import common
import pandas as pd
from _management_api_test import credentials



req = common.requests_get('https://data.bs.ch/api/management/v2/pages/test-hester/',
                            auth=(credentials.username, credentials.password)
                          )
file = req.json()
print(file)




# test how to change page
data_test = '{"title": {"fr": "Nouveau titre de la page", "en": "New page title"}, "description": "The page description", "template": "contact.html", "content": {"html": {"fr": "Contenu de la page", "en": "Page content"}, "css": {"fr": "p { color: black; }", "en": "p { color: black; }"}}, "tags": ["tag1", "tag2"], "restricted": true}'

common.requests_put('https://data.bs.ch/api/management/v2/pages/test-hester/',
                   data=data_test,
                    auth=(credentials.username, credentials.password)
                    )

