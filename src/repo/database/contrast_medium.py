"""
  A visualization of the views and its relationship between
  UNTERS_SCHLUESSEL ^= ACCESSION NUMBER for contrast medium

                             VIEW: A_ABRFALL_LEISTUNG
  +-----------------------------------------+
  |UNTERS_SCHLUESSEL |  LEISTUNG_SCHLUESSEL |
  +-----------------------------------------+
  |                  |                      |
  +-----------------------------------------+
                               |
          +--------------------+
          v                VIEW:
  +-----------------------------------------------------------------+
  | A_LSTERF_DOKUMENTATION| BEMERKUNG     |  BASE_SDSD_MISC_NUMBER_1|
  +-----------------------------------------------------------------+
  |                       |               |                         |
  +-----------------------------------------------------------------+
"""
import logging
import cx_Oracle

def query_contrast_medium(cursor, accession_number):
    sql = """
          SELECT DISTINCT
            A.UNTERS_SCHLUESSEL, B.BEMERKUNG, B.BASE_SDSD_MISC_NUMBER_1
          FROM
            A_LSTERF_DOKUMENTATION B
          INNER JOIN A_ABRFALL_LEISTUNG A
            ON A.LEISTUNG_SCHLUESSEL = B.LEISTUNG_SCHLUESSEL
          WHERE
            A.UNTERS_SCHLUESSEL = :accession_number
          """
    try:
        cursor.execute(sql, accession_number=accession_number)
        row = cursor.fetchone()
        if row is None:
            return None
        else:
            return {'bemerkung': row[1], 'quantity': row[2]}
    except cx_Oracle.DatabaseError as e:
        logging.error('Database error occured')
        logging.error(e)
        return None