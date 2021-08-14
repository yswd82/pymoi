# -*- coding: UTF-8 -*-
from dataclasses import dataclass
import pandas as pd
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
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
    def __init__(self, bind, name: str, id_col: str = DEFAULT_RECORD_ID_COL):
        self.bind = bind
        self.name = name
        self.id_col = id_col

        # TODO: PrimaryKeyがないと正しくテーブル定義を取得できない
        Base = automap_base()
        Base.prepare(self.bind, reflect=True)
        self.Table = getattr(Base.classes, self.name)

        Session = sessionmaker(bind=self.bind)
        self.session = Session()

    # テスト用
    def clear(self):
        self.session.query(self.Table).delete()
        self.session.commit()

    # テスト用
    def read_table(self):
        df = pd.read_sql_table(self.name, self.bind)
        return df

    def columns(self):
        return self.read_table().columns

    @property
    def max_record_id(self):
        res = self.session.query(
            func.max(getattr(self.Table, self.id_col))).scalar()
        return int(res or -1)

    def __prepare_data(self, data, id_col, delete_flag_col, first_id):
        if id_col in data.columns or delete_flag_col in data.columns:
            raise Exception

        data[id_col] = range(first_id, first_id+len(data))
        data[delete_flag_col] = 0
        return data

    def execute(self, dataframe_or_reader, overwrite: OverwriteParameter = None):
        if isinstance(dataframe_or_reader, pd.DataFrame):
            data = dataframe_or_reader
        elif isinstance(dataframe_or_reader, PyMoiReader):
            data = dataframe_or_reader.read()
        else:
            ValueError(
                "dataframe_or_reader must be pandas DataFrame or PyMoiReader")
            return

        # レコードIDと削除フラグを追加
        if overwrite:
            latest_record_id = self.max_record_id
            data = self.__prepare_data(
                data, overwrite.id_col, overwrite.delete_flag_col, latest_record_id+1)

        # DataFrameをinsert
        try:
            for row in data.itertuples(index=False):
                params = {k: v for k, v in zip(data.columns, row)}
                insert_row = self.Table(**params)

                self.session.add(insert_row)

            self.session.commit()
        except Exception as e:
            print("Exception:", e)
            self.session.rollback()
            return

        # 論理または物理削除
        if overwrite:
            self.__delete_process(data, overwrite, latest_record_id)

        self.session.commit()

    def __delete_process(self, data, overwrite, latest_record_id):
        # 未取消かつ取込前までのレコードを準備する
        stmt = self.session.query(self.Table).filter(
            getattr(self.Table, overwrite.delete_flag_col) == 0).filter(getattr(self.Table, overwrite.id_col) <= latest_record_id)

        # 上書き条件に指定された全列にマッチするレコードを検索
        for owkey in overwrite.keys:
            stmt = stmt.filter(
                getattr(self.Table, owkey).in_(data[owkey].to_list()))

        # 削除対象レコードのIDを取得
        delete_record_id = [getattr(_, overwrite.id_col) for _ in stmt]

        # 削除対象レコードを論理または物理削除
        stmt = self.session.query(self.Table).filter(
            getattr(self.Table, overwrite.id_col).in_(delete_record_id))

        if overwrite.mode == 'logical':
            stmt.update({overwrite.delete_flag_col: 1})
        elif overwrite.mode == 'physical':
            stmt.delete()

    def __param_generator(self, data):
        for row in data.itertuples(index=False):
            yield {k: v for k, v in zip(data.columns, row)}
