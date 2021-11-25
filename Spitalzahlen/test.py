import pandas as pd
import requests
import credentials



url = "https://qs.ies.admin.ch/sap/opu/odata/ITIES/ODATA_HOKA_SRV/$metadata?sap-client=503"

payload = {}
headers = {
  'Authorization': credentials.authorization_qs}

response = requests.request("GET", url, headers=headers, data=payload)

print(response.text)
print(response.headers)

url = "https://qs.ies.admin.ch/sap/opu/odata/ITIES/ODATA_HOKA_SRV/HospCapAdultSet?$format=json&sap-client=503"
response = requests.request("GET", url, headers=headers, data=payload)
#print(response.json())

url2 = "https://qs.ies.admin.ch/sap/opu/odata/ITIES/ODATA_HOKA_SRV/HospCapAdultSet?$format=json&sap-client=503&$filter=(( CapacStamp gt datetime'2021-11-12T00:00:00' or CapacStamp lt datetime'2021-11-19T23:59:59') and (NoauResid eq '00000000000000047212')  )"
response = requests.request("GET", url2, headers=headers, data=payload)
print(response.text)

results = response.json()["d"]["results"]

# display all columns of dataframe
#pd.set_option('display.max_columns', None)

df = pd.DataFrame(results)

print(df.head())

# put timestamp as index
df = df.set_index(keys=["CapacDate", "CapacTime"])

print(df.head())

print([str(x) for x in df.columns])

columns = [str(x) for x in df.columns]


"""
Total_Betten = df['TotalAllBeds']
Total_Betten_COVID = df['TotalAllBedsC19']
Betriebene_IS_Betten = df['OperIcuBeds']
Betrieben-IS_Betten_COVID = df['OperIcuBedsC19']
Beatmete_IS_Betten = df['VentIcuBeds']
Betriebene_ECMO_Betten = df[''], not available for USB at the moment, we take ECMO = 5...
Betriebene_IMCU_Betten = df['OperImcBeds']
Betriebene_IMCU_Betten_COVID = df['OperImcBedsC19']
Total_Pat = df['TotalAllPats']
Total_Pat_COVID = df['TotalAllPatsC19']
Total_IS_Pat = df['TotalIcuPats']
Total_IS_Pat_COVID = df['TotalIcuPatsC19']
Beatmete_IS_Pat = df['VentIcuPats']
Total_ECMO_Pat = df['EcmoPats'], note: ECMO is only for USB
Total_IMCU_Pat = df['TotalImcPats']
Total_IMCU_Pat_COVID = df['TotalImcPatsC19']
"""