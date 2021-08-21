# -*- coding: UTF-8 -*-
from sqlalchemy import create_engine


def enginefactory_mssql_pyodbc(server, port, database, driver, trusted_connection=None):
    return create_engine(
        f"mssql+pyodbc://{server}:{port}/{database}?trusted_connection=yes&driver={driver}")
