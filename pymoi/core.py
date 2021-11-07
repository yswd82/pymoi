# -*- coding: UTF-8 -*-
from dataclasses import dataclass
from os import name
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
from pymoi.reader import PyMoiReader, CsvReader, ExcelReader, FixedParameter, CellParameter, DirectionParameter, RepeatParameter
from pymoi._version import __version__
import json

# default name of columns
DEFAULT_RECORD_ID_COL = 'record_id'
DEFAULT_DELETE_FLAG_COL = 'is_deleted'


@dataclass
class OverwriteParameter:
    """上書き処理を行うパラメータの基底クラス
    """
    mode: str
    keys: list
    id_col: str = DEFAULT_RECORD_ID_COL
    delete_flag_col: str = DEFAULT_DELETE_FLAG_COL


class PyMoi:
    def __init__(self, bind, name: str):
        self.bind = bind
        self.name = name

        self.target_table = self._create_table_class(bind, name)

        self.reader = None

    def _create_table_class(self, bind, name):
        """書き込み対象のテーブルクラスを作成する

        Args:
            bind ([type]): SQLAlchemyのbind
            name ([type]): テーブル名

        Returns:
            [type]: テーブルクラス
        """
        Base = declarative_base(bind)

        namespace = {'__tablename__': name,
                     '__table_args__': {'autoload': True, 'autoload_with': bind}, }
        class_ = type('TableClass', (Base,), namespace)

        return class_

    def clear(self):
        """テーブル内容を削除する
        """

        Session = sessionmaker(bind=self.bind)
        session = Session()
        session.query(self.target_table).delete()
        session.commit()

    def read_table(self):
        """テーブルのデータを取得する

        Returns:
            pandas.DataFrame: テーブルのデータ
        """
        df = pd.read_sql_table(self.name, self.bind)
        return df

    def columns(self):
        """テーブルのカラム名を取得する

        Returns:
            [type]: [description]
        """
        _columns = self.read_table().columns
        return _columns

    def max_record_id(self, id_col):
        """テーブルの最大レコードIDを取得する

        Args:
            id_col ([type]): レコードIDの列名

        Returns:
            int: レコードIDの最大値
        """
        # id_col列の最大値を取得
        Session = sessionmaker(bind=self.bind)
        session = Session()

        _max_id_col = session.query(
            func.max(getattr(self.target_table, id_col))).scalar()
        return int(_max_id_col or -1)

    def __prepare_data(self, data, id_col, delete_flag_col, first_id):
        """投入データにレコードIDと削除フラグを追加する

        Args:
            data ([type]): [description]
            id_col ([type]): [description]
            delete_flag_col ([type]): [description]
            first_id ([type]): [description]

        Raises:
            AttributeError: [description]
            AttributeError: [description]

        Returns:
            [type]: [description]
        """
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
        """データを書き込む

        Args:
            dataframe_or_reader ([type]): [description]
            overwrite (OverwriteParameter, optional): [description]. Defaults to None.
        """
        if isinstance(dataframe_or_reader, pd.DataFrame):
            data = dataframe_or_reader
        elif isinstance(dataframe_or_reader, PyMoiReader):
            self.reader = dataframe_or_reader
            data = self.reader.read()
        else:
            ValueError(
                "dataframe_or_reader must be pandas.DataFrame or pymoi.reader.PyMoiReader")
            return

        # レコードIDと削除フラグを追加
        if overwrite:
            latest_record_id: int = self.max_record_id(overwrite.id_col)
            data = self.__prepare_data(
                data, overwrite.id_col, overwrite.delete_flag_col, latest_record_id+1)

        # DataFrameをinsert
        Session = sessionmaker(bind=self.bind)
        session = Session()

        try:
            for param in self._param_generator(data):
                insert_row = self.target_table(**param)

                session.add(insert_row)
        except Exception as e:
            print("Exception:", e)
            session.rollback()
            return

        session.commit()

        # 論理または物理削除
        if overwrite:
            self.__delete_process(
                data, overwrite, latest_record_id, session)

        session.commit()

    def __delete_process(self, data, overwrite, latest_record_id: int, session):
        """上書き有効の場合に取消・削除処理を実行する

        Args:
            data ([type]): [description]
            overwrite ([type]): [description]
            latest_record_id (int): [description]
            session ([type]): [description]
        """
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

    def export_config(self):
        if isinstance(self.reader, PyMoiReader):
            cfg = {
                'app': 'pymoi',
                'version': __version__,
                'table_name': self.name,
                'created_at': '2021-11-07',
                'created_by': 'hogehoge'
            }
            cfg.update(self.reader.export_config())
            return cfg


class PyMoiTemplateFactory:
    def create(self, file_path: str):
        js = json.loads(file_path)

        if js["reader_type"] == 'csv':
            reader = CsvReader(
                fullname=js["fullname"],
                delimiter=js["delimiter"],
                quotechar=js["quotechar"]
            )
        elif js["reader_type"] == 'excel':
            reader = ExcelReader(
                fullname=js["fullname"],
                seek_start=js["seek_start"],
                unit_row=js["unit_row"],
                sheetname=js["sheetname"],
                names=js["names"],
            )

            reader.parameters = [PyMoiParameterFactory(
                p).create() for p in js["parameters"]]


class PyMoiParameterFactory:
    def __init__(self, parameter: dict):
        self.parameter = parameter

    def create(self):
        if self.parameter["type"] == 'fixed':
            return FixedParameter(self.parameter["value"])
        elif self.parameter["type"] == 'cell':
            return CellParameter(self.parameter["cell"])
        elif self.parameter["type"] == 'direction':
            return DirectionParameter(
                self.parameter["line"],
                self.parameter["column"],
                self.parameter["number"]
            )
        elif self.parameter["type"] == 'repeat':
            return RepeatParameter(
                self.parameter["line"],
                self.parameter["column"],
                self.parameter["number"]
            )
