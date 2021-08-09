# -*- coding: UTF-8 -*-
from datetime import datetime
from dataclasses import dataclass
from typing import List
import pandas as pd
import xlwings as xw


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

    def get_max_record_id(self, name: str):
        cursor = self._con.cursor()
        cursor.execute(f"SELECT max(record_id) FROM {name}")
        _ = cursor.fetchone()
        cursor.close()
        return max(_)

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

    def __del__(self):
        self._con.close()


@dataclass
class OverwriteCommand:
    method: str
    key: list
    logical_delete_col_name: str
