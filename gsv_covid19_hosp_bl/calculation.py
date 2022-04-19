import pandas as pd


def import_numbers(df):
    total_betten = df['TotalAllBeds']
    total_betten_covid = df['TotalAllBedsC19']
    betriebene_is_betten = df['OperIcuBeds']
    betriebene_is_betten_covid = df['OperIcuBedsC19']
    beatmete_is_betten = df['VentIcuBeds']
    betriebene_imcu_betten = df['OperImcBeds']
    betriebene_imcu_betten_covid = df['OperImcBedsC19']
    total_pat = df['TotalAllPats']
    total_pat_covid = df['TotalAllPatsC19']
    total_is_pat = df['TotalIcuPats']
    total_is_pat_covid = df['TotalIcuPatsC19']
    beatmete_is_pat = df['VentIcuPats']
    total_imcu_pat = df['TotalImcPats']
    total_imcu_pat_covid = df['TotalImcPatsC19']
    beatmete_imcu_pat_covid = df['VentImcPatsC19']

    a = total_betten
    b = betriebene_imcu_betten
    c = betriebene_is_betten
    d = beatmete_is_betten
    e1 = total_pat
    e2 = total_imcu_pat
    f = total_is_pat
    g = beatmete_is_pat
    h1 = total_pat_covid
    h2 = total_imcu_pat_covid
    i = total_is_pat_covid
    i2 = beatmete_imcu_pat_covid
    j = total_imcu_pat_covid

    return a, b, c, d, e1, e2, f, g, h1, h2, i, i2, j

def total_betten_frei(df):
    a, b, c, d, e1, e2, f, g, h1, h2, i, i2, j  = import_numbers(df)
    if df['Hospital'] == 'Arlesheim':
        return a-b-(e1-e2)
    else:
        return a-c-(e1-f)


# Note: I took the below two functions from the Excel file, it could probably be simplified...
def ips_ohne_beatmung(df):
    a, b, c, d, e1, e2, f, g, h1, h2, i, i2, j  = import_numbers(df)
    if c == g:
        return (c-d) -(f-g)
    elif (c-d) - (f-g)<0:
        return 0
    else:
        return (c-d) - (f-g)

def ips_mit_beatmung(df):
    a, b, c, d, e1, e2, f, g, h1, h2, i, i2, j  = import_numbers(df)
    if c == g:
        return 0
    elif (c-d)-(f-g)<0:
        return (d-g) + ((c-d) - (f-g))
    else:
        return d-g


def calculate_numbers(ies_numbers):
    df = ies_numbers
    df_coreport = pd.DataFrame()
    df_coreport[["Hospital", "NoauResid", "CapacDate", "CapacTime"]] = \
        df[["Hospital", "NoauResid", "CapacDate", "CapacTime"]]

    a, b, c, d, e1, e2, f, g, h1, h2, i, i2, j  = import_numbers(df)

    df_coreport['Bettenanzahl frei "Normal"'] = df.apply(total_betten_frei, axis=1)
    df_coreport['Bettenanzahl frei "IMCU"'] = b-e2
    df_coreport['Bettenanzahl belegt "Normal"'] = e1-e2
    df_coreport['Bettenanzahl belegt "IMCU"'] = e2
    df_coreport['Anzahl Patienten Normal COVID'] = h1-h2



    df_coreport['Anzahl Patienten IMCU COVID mit Beatmung'] = i2
    df_coreport['Anzahl Patienten IMCU COVID ohne Beatmung'] = h2-i2
    df_coreport['Bettenanzahl IPS ohne Beatmung'] = c - d
    df_coreport['Bettenanzahl frei "IPS ohne Beatmung"'] = df.apply(ips_ohne_beatmung, axis=1)
    df_coreport['Bettenanzahl frei "IPS mit Beatmung"'] = df.apply(ips_mit_beatmung, axis=1)
    df_coreport['Bettenanzahl belegt "Normal" inkl. COVID Verdachtsfälle'] = e1-f -(h1-i)
    df_coreport['Bettenanzahl belegt "Normal" COVID'] = h1 - i
    df_coreport['Bettenanzahl belegt "IPS ohne Beatmung"'] = df_coreport['Bettenanzahl IPS ohne Beatmung'] - df_coreport['Bettenanzahl frei "IPS ohne Beatmung"']
    df_coreport['Bettenanzahl belegt "IPS mit Beatmung"'] = d - df_coreport['Bettenanzahl frei "IPS mit Beatmung"']
    df_coreport['Anzahl Patienten "IPS nicht Beatmet" inkl. COVID Verdachtsfälle'] = (f-g)-(i-j)
    df_coreport['Anzahl Patienten "IPS Beatmet" inkl. COVID Verdachtsfälle'] = g - j
    df_coreport['Anzahl Patienten "IPS nicht Beatmet" COVID'] = i - j
    df_coreport['Anzahl Patienten "IPS Beatmet" COVID'] = j
    return df_coreport


"""
Arlesheim:
 A = Betten total
 B = IMCU Betten
 D = Patienten Total
 E = IMCU patienten
 G = COVID patienten total
 H = IMCU Covid Patienten

'Bettenanzahl frei "Normal"' = A-B-(D-E)
'Bettenanzahl frei "IMCU"' = B-E
'Bettenanzahl belegt "Normal"' = D-E
'Bettenanzahl belegt "IMCU"' = E
'Anzahl Patienten Normal COVID' = G-H
'Anzahl Patienten IMCU COVID mit Beatmung' = I
'Anzahl Patienten IMCU COVID ohne Beatmung' = H-I




Bruderholz/Liestal:
B = Betten Total
C = Betr. IS-Betten
D = Beatm. IS-Betten
E = Total Pat
F = Total IS-Pat
G = Beatm. IS-Pat
H = Total C19 Pat
I = C19 IS-Pat
J = C19 Beatm. IS-Pat.

Bettenanzahl IPS ohne Beatmung = C - D


'Bettenanzahl frei "Normal"' = B-C-(E-F)

'Bettenanzahl frei "IPS ohne Beatmung"' =
if C=G:
    Bettenanzahl IPS ohne Beatmung-(F-G)
elif Bettenanzahl IPS ohne Beatmung-(F-G)<0:
    0
else:
    Bettenanzahl IPS ohne Beatmung-(F-G)
??

'Bettenanzahl frei "IPS mit Beatmung"' =
if C=G:
    0
elif Bettenanzahl IPS ohne Beatmung-(F-G)<0:
    (D-G) + (Bettenanzahl IPS ohne Beatmung-(F-G))
else:
    D-G

'Bettenanzahl belegt "Normal" inkl. COVID Verdachtsfälle' = E-F -(H-I)
'Bettenanzahl belegt "Normal" COVID' = H - I
'Bettenanzahl belegt "IPS ohne Beatmung"' = E - 'Bettenanzahl frei "IPS ohne Beatmung"'
'Bettenanzahl belegt "IPS mit Beatmung"' = D - 'Bettenanzahl frei "IPS mit Beatmung"'
'Anzahl Patienten "IPS nicht Beatmet" inkl. COVID Verdachtsfälle' = (F-G)-(I-J)
'Anzahl Patienten "IPS Beatmet" inkl. COVID Verdachtsfälle' = G - J
'Anzahl Patienten "IPS nicht Beatmet" COVID' = I - J
'Anzahl Patienten "IPS Beatmet" COVID' = J



"""

if __name__ == "__main__":
    pass