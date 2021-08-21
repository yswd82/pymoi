# -*- coding: UTF-8 -*-
from dataclasses import dataclass
import pandas as pd
from sqlalchemy import Table, Column, Integer, String, create_engine, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mssql import \
    BIGINT, BINARY, BIT, CHAR, DATE, DATETIME, DATETIME2, \
    DATETIMEOFFSET, DECIMAL, FLOAT, IMAGE, INTEGER, JSON, MONEY, \
    NCHAR, NTEXT, NUMERIC, NVARCHAR, REAL, SMALLDATETIME, \
    SMALLINT, SMALLMONEY, SQL_VARIANT, TEXT, TIME, \
    TIMESTAMP, TINYINT, UNIQUEIDENTIFIER, VARBINARY, VARCHAR
from sqlalchemy.types import String, Integer, Numeric, Date, DateTime, Float
from pymoi.reader import PyMoiReader


DEFAULT_RECORD_ID_COL = 'record_id'
DEFAULT_DELETE_FLAG_COL = 'is_deleted'


@dataclass
class OverwriteParameter:
    mode: str
    keys: list
    id_col: str = DEFAULT_RECORD_ID_COL
    delete_flag_col: str = DEFAULT_DELETE_FLAG_COL


class PyMoi:
    def __init__(self, bind, name: str):
        self.bind = bind
        self.name = name

        self.target_table = self._create_table_class(bind, name)

    def _create_table_class(self, bind, name):
        Base = declarative_base(bind)

        namespace = {'__tablename__': name,
                     '__table_args__': {'autoload': True, 'autoload_with': bind}, }
        class_ = type('TableClass', (Base,), namespace)

        return class_

    def clear(self):
        Session = sessionmaker(bind=self.bind)
        session = Session()
        session.query(self.target_table).delete()
        session.commit()

    def read_table(self):
        df = pd.read_sql_table(self.name, self.bind)
        return df

    def columns(self):
        # 列名のリストを取得
        _columns = self.read_table().columns
        return _columns

    def max_record_id(self, id_col):
        # id_col列の最大値を取得
        Session = sessionmaker(bind=self.bind)
        session = Session()

        _max_id_col = session.query(
            func.max(getattr(self.target_table, id_col))).scalar()
        return int(_max_id_col or -1)

    def __prepare_data(self, data, id_col, delete_flag_col, first_id):
        # 投入データにid_col列が存在した場合はエラー
        if id_col in data.columns:
            raise AttributeError(f"column name '{id_col}' is already exists")
        else:
            data[id_col] = range(first_id, first_id+len(data))

        # 投入データにdelete_flag_col列が存在した場合はエラー
        if delete_flag_col in data.columns:
            raise AttributeError(
                f"column name '{delete_flag_col}' is already exists")
        else:
            data[delete_flag_col] = 0

        return data

    def execute(self, dataframe_or_reader, overwrite: OverwriteParameter = None):
        if isinstance(dataframe_or_reader, pd.DataFrame):
            data = dataframe_or_reader
        elif isinstance(dataframe_or_reader, PyMoiReader):
            data = dataframe_or_reader.read()
        else:
            ValueError(
                "dataframe_or_reader must be pandas.DataFrame or pymoi.reader.PyMoiReader")
            return

        # レコードIDと削除フラグを追加
        if overwrite:
            latest_record_id = self.max_record_id(overwrite.id_col)
            data = self.__prepare_data(
                data, overwrite.id_col, overwrite.delete_flag_col, latest_record_id+1)

        # DataFrameをinsert
        Session = sessionmaker(bind=self.bind)
        session = Session()

        try:
            for param in self._param_generator(data):
                insert_row = self.target_table(**param)

            # for row in data.itertuples(index=False):
            #     params = {k: v for k, v in zip(data.columns, row)}
            #     insert_row = self.target_table(**params)

                session.add(insert_row)
        except Exception as e:
            print("Exception:", e)
            session.rollback()
            return

        session.commit()

        # 論理または物理削除
        if overwrite:
            self.__delete_process(data, overwrite, latest_record_id, session)

        session.commit()

    def __delete_process(self, data, overwrite, latest_record_id, session):
        # 未取消かつ取込前までのレコードを抽出する
        statement = session.query(self.target_table).filter(
            getattr(self.target_table, overwrite.delete_flag_col) == 0).filter(getattr(self.target_table, overwrite.id_col) <= latest_record_id)

        # 上書き条件に指定された全列にマッチするレコードを検索する
        for owkey in overwrite.keys:
            statement = statement.filter(
                getattr(self.target_table, owkey).in_(data[owkey].to_list()))

        # 削除対象レコードのIDを取得する
        delete_record_id = [getattr(_, overwrite.id_col) for _ in statement]

        # 削除対象レコードを論理または物理削除する
        statement = session.query(self.target_table).filter(
            getattr(self.target_table, overwrite.id_col).in_(delete_record_id))

        if overwrite.mode == 'logical':
            statement.update({overwrite.delete_flag_col: 1})
        elif overwrite.mode == 'physical':
            statement.delete()

    def _param_generator(self, data):
        for row in data.itertuples(index=False):
            yield {k: v for k, v in zip(data.columns, row)}
