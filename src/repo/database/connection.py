import logging
import os
import oracledb
from sqlalchemy import create_engine, text


def open_connection():
    # RIS DB connection values
    RIS_DB_HOST = os.getenv("RIS_DB_HOST")
    RIS_DB_PORT = os.getenv("RIS_DB_PORT")
    RIS_DB_SERVICE = os.getenv("RIS_DB_SERVICE")
    RIS_DB_USER = os.getenv("RIS_DB_USER")
    RIS_DB_PASSWORD = os.getenv("RIS_DB_PASSWORD")

    oracledb.init_oracle_client()

    engine = create_engine(
        f"oracle+oracledb://{RIS_DB_USER}:{RIS_DB_PASSWORD}@{RIS_DB_HOST}:{RIS_DB_PORT}/?service_name={RIS_DB_SERVICE}",
        max_identifier_length=128,
    )
    return engine


def secta_cdwh_connection():
    logging.debug('Opening connection to sectra db')
    user = os.getenv("SECTRA_USER")
    password = os.getenv("SECTRA_PASSWORD")
    hostname = os.getenv("SECTRA_HOST")
    port = os.getenv("SECTRA_PORT")
    dbname = os.getenv("SECTRA_DB")
    engine = create_engine(
        f"mssql+pymssql://{user}:{password}@{hostname}:{port}/{dbname}"
    )
    return engine
