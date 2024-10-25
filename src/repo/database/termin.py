import logging

import cx_Oracle


def query_for_termin(cursor, accession_number):
    sql = """
         SELECT
            PATIENT_SCHLUESSEL,
            TERMIN_ANFANG,
            TAETIGKEIT_KUERZEL,
            DAUER,
            pat_name,
            pat_vorname,
            pat_land,
            pat_plz,
            pat_strasse,
            pat_geburtsdatum,
            pat_titel,
            pat_geschlecht
        FROM
            MEDORA.A_TERMIN at2
            JOIN MEDORA.A_PATIENT ap  USING(PATIENT_SCHLUESSEL)
        WHERE
            UNTERS_SCHLUESSEL = :accession_number
          """
    try:
        cursor.execute(sql, accession_number=accession_number)
        cursor.rowfactory = lambda *args: dict(zip([d[0] for d in cursor.description], args))
        row = cursor.fetchone()
        if row is None:
            return None
        else:
            return row
    except cx_Oracle.DatabaseError as e:
        logging.error('Database error occured')
        logging.error(e)
        return None


