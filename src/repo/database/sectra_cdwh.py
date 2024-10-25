import os
import logging

from sqlalchemy import create_engine, text

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


