import os
import logging

from sqlalchemy import create_engine, text
from striprtf.striprtf import rtf_to_text

logger = logging.getLogger(__name__)

def sectra_engine():
    user = os.getenv("SECTRA_USER")
    password = os.getenv("SECTRA_PASSWORD")
    hostname = os.getenv("SECTRA_HOST")
    port = os.getenv("SECTRA_PORT")
    dbname = os.getenv("SECTRA_DB")
    engine = create_engine(
        f"mssql+pymssql://{user}:{password}@{hostname}:{port}/{dbname}"
    )
    return engine


def query_cdwh_status():
    query = """
            SELECT TOP 1
                SDWHLoadJobStatus
            FROM
                SDWHLoad
            WHERE
                CAST(SDWHLoadJobBegin AS DATE) = cast(GETDATE() as date)
            ORDER BY SDWHLoadId DESC 
            """
    engine = sectra_engine()
    with engine.connect() as con:
        result = con.execute(text(query))
        results = result.mappings().all()
        return results[0]


def get_as_txt_from_sectra(engine, accession_number):
    sql = """
        SELECT r.ReportText
        FROM reports r
        WHERE r.reportid = (
            SELECT MAX(er.ExaminationReportReportId)
            FROM examinationreports er
            WHERE er.ExaminationReportExaminationId = (
                SELECT e.ExaminationId
                FROM Examinations e
                WHERE e.ExaminationAccessionNumber = :accession_number
            )
        )
        """
    engine = sectra_engine()
    with engine.connect() as con:
        result = con.execute(text(sql), {"accession_number":accession_number})
        results = result.mappings().all()
        if results:
            return rtf_to_text(results[0]["ReportText"])
        return ""