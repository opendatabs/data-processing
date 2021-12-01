
import requests
import credentials


url = credentials.url_meta


payload = {}
headers = {
  'Authorization': credentials.authorization_live}

response = requests.request("GET", url, headers=headers, data=payload)

print(response.text)
print(response.headers)



"""
response = requests.request("GET", url, headers=headers, data=payload)
print(response.text)
#print(response.raise_for_status())


url = credentials.url_hosp_adults
response = requests.request("GET", url, headers=headers, data=payload)
print(response.text)
"""
url2 = credentials.url_hosp_adults + "&$filter=(( CapacStamp gt datetime'2021-11-27T00:00:00' or CapacStamp lt datetime'2021-11-29T23:59:59') and (NoauResid eq '00000000000000047212')  )"
response = requests.request("GET", url2, headers=headers, data=payload)
print(response.text)


print(response.json())


"""
results = response.json()["d"]["results"]


pd.set_option('display.max_columns', None)

df = pd.DataFrame(results)

print(df.head())

# put timestamp as index
df = df.set_index(keys=["CapacDate", "CapacTime"])

print(df.head())

print([str(x) for x in df.columns])

columns = [str(x) for x in df.columns]

"""