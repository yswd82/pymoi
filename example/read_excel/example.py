# -*- coding: UTF-8 -*-
from pymoi.reader import ExcelReader, FixedParameter, CellParameter, DirectionParameter, RepeatParameter

filename = "example\\read_excel\\data.xlsx"

myreader = ExcelReader(filename, seek_start="B6", sheetname="Sheet1")

params = [
    FixedParameter(value="hello"),
    CellParameter("B3"),
    DirectionParameter(1, "B", 3),
    RepeatParameter(1, "E", 1),
    FixedParameter("#システム日時"),
    FixedParameter("#システム日付"),
    FixedParameter("#本日"),
]

myreader.parameters = params
df = myreader.get_dataframe()

print(df.head())
