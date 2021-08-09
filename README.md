# pymoi (master of import)

DataFrameをSQLサーバにINSERTするツール

ついでにExcelから柔軟にDataFrameを生成したりする

## How to use

`main.py` 参照

先に `pandas.read_csv()` などでcsvやxlsxからDataFrame化しておいたデータを扱うことに特化します。

## Example(DataFrame to SQL)

```python
import pyodbc
import pandas as pd
from core import PyMoi


# CREATE TABLE [dbo].[pymoi_test](
# 	[col_pk] [int] NOT NULL,
# 	[col_int_nn] [int] NOT NULL,
# 	[col_varchar_nn] [varchar](100) NOT NULL,
# 	[col_decimal] [decimal](15, 3) NULL,
# 	[col_datetime] [datetime] NULL,
# PRIMARY KEY
# (
# 	[col_pk] ASC
# )
# )


# SQL Server 2019
driver = "{ODBC Driver 17 for SQL Server}"
server = "127.0.0.1\SQLEXPRESS"
database = "db"
trusted_connection = "yes"
con_str = f"DRIVER={driver};SERVER={server};DATABASE={database};PORT=1433;Trusted_Connection={trusted_connection};"

con = pyodbc.connect(con_str)

csvfile = "testdata_100000.csv"
df = pd.read_csv(csvfile)

moi = PyMoi(con)
moi.to_sql("dbo.pymoi_test", df)

con.close()
```

## Example(Excel to DataFrame)

サンプルデータ 'data.xlsx'

![サンプルデータ](https://user-images.githubusercontent.com/38760948/128547328-c881c34d-cf6b-4ab4-bd6a-60e328bca9fe.png)

```

# -*- coding: UTF-8 -*-
from core import (
    ExcelReader,
    FixedParameter,
    CellParameter,
    DirectionParameter,
    RepeatParameter,
)

filename = "data.xlsx"

# リーダーを作成
myreader = ExcelReader(filename, seek_start="B6")

# パラメータを作成, 先に指定した順に適用される
params = [
    FixedParameter("hello"),
    CellParameter("B3"),
    DirectionParameter(1, "B", 3),
    RepeatParameter(1, "E", 1),
]

myreader.parameters = params

# ExcelからDataFrameを生成
df = myreader.get_dataframe()

print(df.head())
```

条件
- B6セルから下方に向かってセル値が空白になるまで取得を実施(seek_start)
- 最初の列は固定値で"hello"(FixedParameter)
- 次の列はB3セルの値(CellParameter)
- 次の列はB列から開始して3列分(DirectionParameter)
- 次の列はE列から開始して1列分(空白は直前の値で埋める)(RepeatParameter)

結果

```
       0  1  2    3     4  5
0  hello  A  a  1.0  10.0  x
1  hello  A  b  2.0  20.0  x
2  hello  A  c  3.0  30.0  y
3  hello  A  d  4.0  40.0  y
```


## TODO

- INSERTするテーブルがない場合の対応(先にCREATE TABLEする
- DataFrameとテーブルの列数が一致しない場合の対応(列名変換マッピング, 元データにない列の追加
- INSERT先のテーブルに特定の列(取消フラグ)がある場合にレコードを論理削除する
- INSERTし終わったファイルの自動退避
- 動作内容のセーブとロード(json?
- 複数のINSERTの連続実行(プラン
