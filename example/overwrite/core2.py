# -*- coding: UTF-8 -*-
import pandas as pd
from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.automap import automap_base
# from sqlalchemy.ext.declarative import declarative_base
# from sqlalchemy.inspection import inspect
# from sqlalchemy.schema import Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

dialect = 'mssql'
driver1 = 'pyodbc'
driver = 'ODBC Driver 11 for SQL Server'
host = '127.0.0.1\datahandling'
port = 1433
database = 'db'

engine = create_engine(
    f"{dialect}+{driver1}://{host}:{port}/{database}?trusted_connection=yes&driver={driver}")


class PyMoi2:
    def __init__(self, bind, name: str):
        self.bind = bind
        self.name = name

        Base = automap_base()
        Base.prepare(self.bind, reflect=True)
        self.Table = getattr(Base.classes, self.name)

        Session = sessionmaker(bind=self.bind)
        self.session = Session()

    # テスト用
    def clear(self):
        self.session.query(self.Table).delete()
        self.session.commit()

    def get_max_record_id(self, id_col: str = "record_id"):
        res = self.session.query(
            func.max(getattr(self.Table, id_col))).scalar()
        return int(res or -1)

    def __prepare_data(self, data, id_col, delete_flag_col, first_id):
        if id_col in data.columns or delete_flag_col in data.columns:
            raise Exception

        data[id_col] = range(first_id, first_id+len(data))
        data[delete_flag_col] = 0
        return data

    def __update_delete_flag(self, data, overwrite, id_col, delete_flag_col, last_id):

        # 未取消かつ取込前までのレコードを準備する
        stmt = self.session.query(self.Table).filter(
            getattr(self.Table, delete_flag_col) == 0).filter(getattr(self.Table, id_col) <= last_id)

        # 上書き条件に指定された全列にマッチするレコードを検索
        for owkey in overwrite:
            stmt = stmt.filter(
                getattr(self.Table, owkey).in_(data[owkey].to_list()))

        # 削除対象レコードのIDを取得
        delete_record_id = [getattr(_, id_col) for _ in stmt]

        # 削除対象レコードの削除フラグを更新
        stmt = self.session.query(self.Table).filter(
            getattr(self.Table, id_col).in_(delete_record_id)).update({delete_flag_col: 1})

    def execute(self, data: pd.DataFrame, overwrite=None, id_col: str = "record_id", delete_flag_col: str = "is_deleted"):

        last_id = self.get_max_record_id()
        # print("last_id=", last_id)

        # レコードIDと削除フラグを追加
        if overwrite:
            data = self.__prepare_data(
                data, id_col, delete_flag_col, last_id+1)

        # DataFrameをinsert
        try:
            for row in data.itertuples(index=False):
                params = {k: v for k, v in zip(data.columns, row)}
                insert_row = self.Table(**params)

                self.session.add(insert_row)

            self.session.commit()
        except Exception as e:
            print(e)
            self.session.rollback()
            return

        if overwrite:
            self.__update_delete_flag(
                data, overwrite, id_col, delete_flag_col, last_id)

        self.session.commit()


pm2 = PyMoi2(bind=engine, name='pymoi_example')
pm2.clear()

header_col = ['fid', 'fdate', 'fcode', 'fprice', 'famount']

# insert initial data
csvfile = "table_init.csv"
df = pd.read_csv(csvfile, header=None, names=header_col)
# df['record_id'] = range(len(df))
# df['is_deleted'] = 0

# initデータ投入
owkey = ['fid', 'fcode']
pm2.execute(df, overwrite=owkey)


# # initデータ投入
# pm2.execute(df)

rid = pm2.get_max_record_id()
print("maxid/2=", rid)


# insert additional data
csvfile_ow1 = "overwrite_case1.csv"
df_ow1 = pd.read_csv(csvfile_ow1, header=None, names=header_col)

# maxid = pm2.get_max_record_id()

# print('maxid=', maxid)
# print('len1=', len(df_ow1))

# df_ow1['record_id'] = range(maxid+1, maxid+1 + len(df_ow1))
# df_ow1['is_deleted'] = 0

# addデータ投入
pm2.execute(df_ow1, overwrite=owkey)

rid = pm2.get_max_record_id()
print("maxid/2=", rid)
