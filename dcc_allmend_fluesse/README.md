This folder generates the dataset "Allmendbewilligungen Nähe Basler Flüsse" (https://data.bs.ch/explore/assets/100556/)
through the pyhton etl (extract, transform, load) script in etl.py.
The goal of this dataset is to add the relevant events into the BachApp.

**Extract:** The basis for the script are the datasets "Allmendbewilligungen" (https://data.bs.ch/explore/assets/100018/) and "Gewässernetz: Gewässerachsen" (https://data.bs.ch/explore/assets/100261/).

**Transform**: We only consider permits that have an event type that is either "Veranstaltung", "Aktivität" or "Festivität". We also group permits that have the same "BegehrenID" and "Bezeichnung", since they belong to the same events and merge consecutive event dates into one event. Lastly, only events whose location has at most a distance of 150m to the middle lines of the rivers Rhein, Wiese and Birs are kept.
The resulting dataset has the columns "Bezeichnung", "Belegstatus", "Entscheid-Datum", "Datum_von", "Datum_bis", "Nähe_Flüsse" and "Link".
The first three are taken from the original permit. If there multiple entries with different "Belegstatus" were merged together, they appear as a list. 
The dates are calculated by merging consecutive days in teh same event. The column "Nähe_Flüsse" contains a list of the rivers this event is close to. And lastly "Link" links the event to all permits in the original data set that have been merged together in one event. 

**Load**: The resulting dataset is uploaded directly to data.bs.ch, if there have been changes. The script runs automatically twice a day.

