# -*- coding: UTF-8 -*-
from sqlalchemy import create_engine


def enginefactory_mssql_pyodbc(server, port, database, driver, trusted_connection=None):
    # driver = 'ODBC Driver 11 for SQL Server'
    # host = '127.0.0.1\datahandling'
    # port = 1433
    # database = 'db'

    return create_engine(
        f"mssql+pyodbc://{server}:{port}/{database}?trusted_connection=yes&driver={driver}")
