import logging
import cx_Oracle


def open_connection(host, port, service, user, password):
    logging.debug('Opening connection to ris db')
    dsn = cx_Oracle.makedsn(host, port, service_name=service)
    con = cx_Oracle.connect(user, password, dsn)
    return con
