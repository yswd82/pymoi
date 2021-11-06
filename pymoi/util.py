# -*- coding: UTF-8 -*-
from sqlalchemy import create_engine


def enginefactory_mssql_pyodbc(server, port, database, driver, trusted_connection=None):
    """SQL Alchemy engine factory for Microsoft SQL Server using pyodbc.

    Args:
        server ([type]): Server name or IP address.
        port ([type]): Port number.
        database ([type]): Database name.
        driver ([type]): ODBC driver name.
        trusted_connection ([type], optional): If Yes, use trusted connection. Defaults to None.

    Returns:
        [type]: SQL Alchemy engine.
    """
    return create_engine(
        f"mssql+pyodbc://{server}:{port}/{database}?driver={driver}&trusted_connection=Yes")
