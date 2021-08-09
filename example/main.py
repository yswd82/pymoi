import pyodbc
import pandas as pd
from core import PyMoi

# SQL Server 2019
driver = "{ODBC Driver 17 for SQL Server}"
server = "127.0.0.1\SQLEXPRESS"
database = "db"
trusted_connection = "yes"
con_str = f"DRIVER={driver};SERVER={server};DATABASE={database};PORT=1433;Trusted_Connection={trusted_connection};"

# make con object
con = pyodbc.connect(con_str)

csvfile = "pymoi\\data\\testdata_100000.csv"
df = pd.read_csv(csvfile)

moi = PyMoi(con)
moi.to_sql("dbo.pymoi_test", df)
