# -*- coding: UTF-8 -*-
from datetime import datetime
from dataclasses import dataclass
from typing import List
import pandas as pd
import xlwings as xw
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
from pymoi.reader import FixedParameter, CellParameter, DirectionParameter, RepeatParameter, ExcelReader


class PyMoi:
    description_keys = (
        "name",
        "type_code",
        "display_size",
        "internal_size",
        "precision",
        "scale",
        "null_ok",
    )

    def __init__(self, con):
        self._con = con

    def _get_col_info(self, name: str):
        cursor = self._con.cursor()
        cursor.execute(f"SELECT * FROM {name} WHERE 1=0")
        _ = [dict(zip(self.description_keys, col))
             for col in cursor.description]
        cursor.close()
        return _

    def to_sql(self, name: str, dataframe: pd.DataFrame):
        # 文字型または日付型をシングルクォーテーション
        for info, df_col in zip(self._get_col_info(name), dataframe):
            if isinstance(info["type_code"], str) or isinstance(
                info["type_code"], datetime
            ):
                dataframe[df_col] = dataframe[df_col].apply(
                    lambda x: "'" + str(x) + "'"
                )

        escape = ",".join(["?"] * len(dataframe.columns))
        sql = f"insert into {name} values({escape})"

        try:
            cursor = self._con.cursor()
            cursor.executemany(sql, dataframe.values.tolist())

            self._con.commit()

        except Exception as e:
            self._con.rollback()
            raise e

        finally:
            cursor.close()
            self._con.close()


@dataclass
class OverwriteParameter:
    mode: str
    keys: list
    id_col: str = 'record_id'
    delete_flag_col: str = 'is_deleted'


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

    # テスト用
    def show(self, n=5):
        df = pd.read_sql_table(self.name, self.bind)
        print(df.head(n))
        # df.to_sql(self.name, self.bind, index=False, if_exists='append')

        # for row in self.session.query(self.Table).all():
        #     print(vars(row))

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

    def __update_delete_flag2(self, data, overwrite, last_id):
        # 未取消かつ取込前までのレコードを準備する
        stmt = self.session.query(self.Table).filter(
            getattr(self.Table, overwrite.delete_flag_col) == 0).filter(getattr(self.Table, overwrite.id_col) <= last_id)

        # 上書き条件に指定された全列にマッチするレコードを検索
        for owkey in overwrite.keys:
            stmt = stmt.filter(
                getattr(self.Table, owkey).in_(data[owkey].to_list()))

        # 削除対象レコードのIDを取得
        delete_record_id = [getattr(_, overwrite.id_col) for _ in stmt]

        # 削除対象レコードを論理または物理削除
        stmt = self.session.query(self.Table).filter(
            getattr(self.Table, overwrite.id_col).in_(delete_record_id))

        print("mode=", overwrite.mode)
        if overwrite.mode == 'logical':
            stmt.update({overwrite.delete_flag_col: 1})
        elif overwrite.mode == 'physical':
            stmt.delete()

    def __param_generator(self, data):
        for row in data.itertuples(index=False):
            yield {k: v for k, v in zip(data.columns, row)}

    def execute(self, data: pd.DataFrame, overwrite=None, id_col: str = "record_id", delete_flag_col: str = "is_deleted"):

        # レコードIDと削除フラグを追加
        if overwrite:
            last_id = self.get_max_record_id()
            data = self.__prepare_data(
                data, id_col, delete_flag_col, last_id+1)

        # DataFrameをinsert
        try:
            # coreは無効化しておく
            enable_core = False
            insert_rows = []

            for row in data.itertuples(index=False):
                params = {k: v for k, v in zip(data.columns, row)}
                insert_row = self.Table(**params)

                # 高速化のためsqlalchemy.coreを使用
                if enable_core:
                    insert_rows.append(params)
                else:
                    self.session.add(insert_row)

            # 高速化のためsqlalchemy.coreを使用
            if enable_core:
                self.session.execute(
                    self.Table.__table__.insert(), insert_rows)

            self.session.commit()
        except Exception as e:
            print("Exception:", e)
            self.session.rollback()
            return

        # 削除フラグを更新
        if overwrite:
            self.__update_delete_flag(
                data, overwrite, id_col, delete_flag_col, last_id)

        self.session.commit()

    def execute2(self, data: pd.DataFrame, overwrite: OverwriteParameter = None):
        # レコードIDと削除フラグを追加
        if overwrite:
            last_id = self.get_max_record_id()
            data = self.__prepare_data(
                data, overwrite.id_col, overwrite.delete_flag_col, last_id+1)

        # DataFrameをinsert
        try:
            # coreは無効化しておく
            enable_core = False
            insert_rows = []

            for row in data.itertuples(index=False):
                params = {k: v for k, v in zip(data.columns, row)}
                insert_row = self.Table(**params)

                # 高速化のためsqlalchemy.coreを使用
                if enable_core:
                    insert_rows.append(params)
                else:
                    self.session.add(insert_row)

            # 高速化のためsqlalchemy.coreを使用
            if enable_core:
                self.session.execute(
                    self.Table.__table__.insert(), insert_rows)

            self.session.commit()
        except Exception as e:
            print("Exception:", e)
            self.session.rollback()
            return

        # 論理または物理削除
        self.__update_delete_flag2(data, overwrite, last_id)

        self.session.commit()


# ファイル操作機能を統合
class PyMoi3:
    def __init__(self, bind, name: str, reader:):
        self.bind = bind
        self.name = name

        Base = automap_base()
        Base.prepare(self.bind, reflect=True)
        self.Table = getattr(Base.classes, self.name)

        Session = sessionmaker(bind=self.bind)
        self.session = Session()

    def execute(self):
        pass

    @property
    def read(self):
        return

    @property
    def max_record_id(self):
        return
