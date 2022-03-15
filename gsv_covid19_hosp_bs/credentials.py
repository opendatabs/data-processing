base_path = '/code/data-processing/gsv_covid19_hosp_bs'
path_log_csv = f'{base_path}/log/log_file.csv'
path_log_pkl = f'{base_path}/log/log_file.pkl'


username = "EKBSAPI002"
password_qs = "dW,&^2 L6<hNV3^#@75OaG]w\[}1"
password_live = "f^Ox2n]#v73Q3b55626MNtnqV4f|"

authorization_qs = 'Basic RUtCU0FQSTAwMjpkVywmXjIgTDY8aE5WM14jQDc1T2FHXXdcW30x'
authorization_live = 'Basic RUtCU0FQSTAwMjpmXk94Mm5dI3Y3M1EzYjU1NjI2TU50bnFWNGZ8'

email_server = 'mail.bs.ch'
email_server_outlook = 'smtp-mail.outlook.com'
email = 'opendata@bs.ch'
password_email = ''

email_test = ''
password_test = ''

username_coreport = 'jonas.bieri@bs.ch'
password_coreport = 'adj!YIV9'

username_coreport_test = 'Opendata@bs.ch'
password_coreport_test = 'KnvhHy7XCbVX'

url_qs_meta = "https://qs.ies.admin.ch/sap/opu/odata/ITIES/ODATA_HOKA_SRV/$metadata?sap-client=503"
url_qs_hosp_adults = "https://qs.ies.admin.ch/sap/opu/odata/ITIES/ODATA_HOKA_SRV/HospCapAdultSet?$format=json&sap-client=503"
url_qs_hosp_children = "https://qs.ies.admin.ch/sap/opu/odata/ITIES/ODATA_HOKA_SRV/HospCapPediatricSet?$format=json&sap-client=503"
url_meta = "https://www.ies.admin.ch/sap/opu/odata/ITIES/ODATA_HOKA_SRV/$metadata"
url_hosp_adults = "https://www.ies.admin.ch/sap/opu/odata/ITIES/ODATA_HOKA_SRV/HospCapAdultSet?$format=json"
url_hosp_children = "https://www.ies.admin.ch/sap/opu/odata/ITIES/ODATA_HOKA_SRV/HospCapPediatricSet?$format=json"

url_coreport = 'https://bl.coreport.ch/de/reports/api/submit/'


# test api:https://blcoreportch-stage.ch.aldryn.io/de/reports/api/datapoints/?organization=claraspital-erweitert&timeslot=20-12-2021&question=Bettenanzahl+belegt+%22IPS+mit+Beatmung%22
url_coreport_test_api = 'https://blcoreportch-stage.ch.aldryn.io/de/reports/api/datapoints/?format=json'

# live api example: https://bl.coreport.ch/de/reports/api/datapoints/?format=json&organization=claraspital-erweitert&timeslot=18-02-2022&question=Bettenanzahl+belegt+%22IPS+mit+Beatmung%22
url_coreport_api = 'https://bl.coreport.ch/de/reports/api/datapoints/?format=json'

url_login_coreport = f'https://bl.coreport.ch/de/accounts/login'

url_coreport_clara = 'https://bl.coreport.ch/de/reports/claraspital-erweitert/pending/'
url_coreport_ukbb ='https://bl.coreport.ch/de/reports/ukbb-erweitert/pending/'
url_coreport_usb = 'https://bl.coreport.ch/de/reports/unispital-basel-usb-erweitert/pending/'
url_coreport_test = 'https://bl.coreport.ch/de/reports/test-org-dev/pending/'

dict_organization = {'Clara':'claraspital-erweitert', 'UKBB':'ukbb-erweitert', 'USB':'unispital-basel-usb-erweitert' }


dict_hosp = {"USB": "'00000000000000047212'",
             "Clara": "'00000000000000154894'",
             "UKBB": "'00000000000000051818'"}


email_receivers = ['hester.pieters@bs.ch', 'jonas.bieri@bs.ch', 'boris.djakovic@bs.ch', 'davide.zollino@bs.ch', 'jonas.eckenfels@bs.ch', 'Aref.Al-Debi@bs.ch']


IES_emailadresses = {'Clara': 'stephan.steuer@claraspital.ch; lukas.merki@claraspital.ch; rico.ehms@claraspital.ch; michael.albrecht@claraspital.ch',
'UKBB': 'philipp.stoll@ukbb.ch',
'USB': 'christian.abshagen@usb.ch; rainer.gehrisch@usb.ch'}


phone_clara = "Hauptverantwortlicher Notfall(IES)" \
    "\n" \
    "Herr Stephan Steuer" \
    "\n" \
    "Tel.Empfang + 41 61 685 85 85" \
    "\n" \
    "Tel.Notfall + 41 61 685 83 33" \
    "   \n\n" \
    "Stv.’s von Herr Stephan Steuer" \
    "\n" \
    "Frau Sibylle Felber"\
    "\n" \
    "Tel. + 41 61 685 86 75"

phone_UKBB = "Herr Philipp Stoll" \
             "\n" \
             "Tel. +41 61 704 27 11" \
             "\n" \
             "Mobil +41 79 225 03 41"

phone_USB = "Herr Christian Abshagen" \
            "\n" \
            "Mobil + 41 78 911 79 59" \
            "   \n\n" \
            "Herr Rainer Gehrisch" \
            "\n" \
            "Mobil + 41 79 815 38 24"

IES_phonenumbers = {'Clara': phone_clara,
                  'UKBB': phone_UKBB,
                  'USB': phone_USB}


Errinerung_IES = "Sehr geehrte Damen und Herren " \
    "   \n\n" \
    "Die heutigen Eingaben im IES sind noch ausstehend, " \
    "wir bitten Sie die Daten schnellstmöglich einzugeben." \
    "    \r\n" \
    "Besten Dank für Ihre Bemühungen und die rasche Bearbeitung. " \
    "   \n\n" \
    "Wir wünschen Ihnen einen schönen Tag." \
    "   \n\n"\
    "Fachstelle für OGD Basel-Stadt " \
    "    \r\n" \
    "Präsidialdepartement des Kantons Basel-Stadt" \
    "    \r\n" \
    "Statistisches Amt Fachstelle OGD" \
    "    \r\n" \
    "Binningerstrasse 6" \
    "    \r\n" \
    "4001 Basel" \
    "   \n\n" \
    "Telefon: +41 61 267 87 17" \
    "    \r\n" \
    "E-Mail: mailto:opendata@bs.ch" \
    "    \r\n" \
    "Internet: www.opendata.bs.ch / www.statistik.bs.ch" \
    "    \r\n" \
    "Datenportal Basel-Stadt: data.bs.ch" \
    "    \r\n" \
    "Twitter: @OpenDataBS"


