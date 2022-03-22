import pandas as pd




def calculate_numbers(ies_numbers):
    df = ies_numbers
    df_coreport = pd.DataFrame()
    df_coreport[["Hospital", "NoauResid", "CapacDate", "CapacTime"]] = \
        df[["Hospital", "NoauResid", "CapacDate", "CapacTime"]]

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


    if hospital == 'Arlesheim':
        A = total_betten
        B = betriebene_imcu_betten
        D = total_pat
        E = total_imcu_pat
        G = total_pat_covid
        H = total_imcu_pat_covid
        'Bettenanzahl frei "Normal"' = A-B-(D-E)
        'Bettenanzahl frei "IMCU"' = B-E
        'Bettenanzahl belegt "Normal"' = D-E
        'Bettenanzahl belegt "IMCU"' = E
        'Anzahl Patienten Normal COVID' = G-H

        ?? what is this
        it seems that we have:
        I = 0
        'Anzahl Patienten IMCU COVID mit Beatmung' = I
        'Anzahl Patienten IMCU COVID ohne Beatmung' = H-I


    else:
        B = total_betten
        C = betriebene_is_betten
        D = beatmete_is_betten
        E = total_pat
        F = total_is_pat
        G = beatmete_is_pat
        H = total_pat_covid
        I = total_is_pat_covid
        need to check:
        J = total_imcu_pat_covid

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