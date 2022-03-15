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