import json
import pandas as pd

def example():
    with open('example_USB.json') as f:
        json_data = json.load(f)

    print(json_data)
    results_USB = json_data["d"]["results"]

    df_USB = pd.DataFrame(results_USB)

    with open('Example_UKBB.json') as f:
        json_data = json.load(f)

    print(json_data)
    results_UKBB = json_data["d"]["results"]

    df_UKBB = pd.DataFrame(results_UKBB)

    print([str(x) for x in df_UKBB.columns])
    print([str(x) for x in df_USB.columns])

    df_UKBB = df_UKBB[["NoauResid", "CapacDateStr", "CapacTimeStr",'TotalAllBeds','TotalAllBedsC19', 'OperIcuBeds','OperIcuBedsC19',
                                  'VentIcuBeds', 'OperImcBeds', 'OperImcBedsC19', 'TotalAllPats', 'TotalAllPatsC19','TotalIcuPats',
                   'TotalIcuPatsC19','VentIcuPats','TotalImcPats','TotalImcPatsC19']]
    df_UKBB = df_UKBB.rename(columns={"CapacDateStr": "CapacDate", "CapacTimeStr": "CapacTime"})
    #df_UKBB = df_UKBB.set_index(keys=["NoauResid", "CapacDate", "CapacTime"])

    print(df_UKBB.head())



    df_USB= df_USB[["NoauResid", "CapacDate", "CapacTime",'TotalAllBeds','TotalAllBedsC19', 'OperIcuBeds','OperIcuBedsC19',
                                  'VentIcuBeds', 'OperImcBeds', 'OperImcBedsC19', 'TotalAllPats', 'TotalAllPatsC19','TotalIcuPats',
                   'TotalIcuPatsC19','VentIcuPats','TotalImcPats','TotalImcPatsC19']]
    #df_USB = df_USB.set_index(keys=["NoauResid", "CapacDate", "CapacTime"])
    print(df_USB.head())


    print(df_UKBB.dtypes)
    print(df_USB.dtypes)

    df_all = pd.concat([df_USB, df_UKBB], keys=['USB', 'UKBB'])

    print(df_all.head)

    return df_all