import pandas as pd
import sqlite3

def csv_to_sqlite(export_file_all):
    df = pd.read_csv(export_file_all)
    # Create a SQLite database
    conn = sqlite3.connect(export_file_all.replace('all_data.csv', 'datasette/smileys.db'))
    cursor = conn.cursor()

    # Create a table with the appropriate column types
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS kapo_smileys (
        id_standort INTEGER,
        Zyklus INTEGER,
        Messung_Datum TEXT,
        Messung_Zeit TEXT,
        V_Einfahrt INTEGER,
        V_Ausfahrt INTEGER,
        Messung_Timestamp TEXT,
        V_Delta INTEGER,
        Strassenname TEXT,
        Geschwindigkeit INTEGER,
        Messung_Phase TEXT,
        Ort_Abkuerzung TEXT,
        Start_Vormessung TEXT,
        Start_Betrieb TEXT,
        Start_Nachmessung TEXT,
        Ende TEXT,
        Messung_Jahr INTEGER,
        Ort TEXT,
        geo_point_2d TEXT,
        geo_shape TEXT,
        Phase TEXT
    )
    '''

    cursor.execute(create_table_query)
    conn.commit()

    # Import the CSV data into the SQLite table
    df.to_sql('kapo_smileys', conn, if_exists='replace', index=False)

    index_queries = [
        "CREATE INDEX IF NOT EXISTS idx_id_standort ON kapo_smileys (id_standort);",
        "CREATE INDEX IF NOT EXISTS idx_Zyklus ON kapo_smileys (Zyklus);",
        "CREATE INDEX IF NOT EXISTS idx_Phase ON kapo_smileys (Phase);",
        "CREATE INDEX IF NOT EXISTS idx_Messung_Datum ON kapo_smileys (Messung_Datum);"
    ]

    for query in index_queries:
        cursor.execute(query)

    # Commit changes and close the connection
    conn.commit()
    conn.close()