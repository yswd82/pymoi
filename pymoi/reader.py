# -*- coding: UTF-8 -*-
from dataclasses import dataclass, field
from typing import List
import pandas as pd
import xlwings as xw
import datetime


@dataclass
class Parameter:
    pass


@dataclass
class StaticParameter(Parameter):
    pass


@dataclass
class DynamicParameter(Parameter):
    pass


@dataclass
class FixedParameter(StaticParameter):
    value: str

    __reserved_params = {
        "#システム日時": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "#システム日付": datetime.datetime.today().strftime('%Y-%m-%d'),
        "#本日": datetime.datetime.today().strftime('%Y-%m-%d'),
    }

    def __post_init__(self):
        self.value = self.__reserved_params.get(self.value, self.value)


@dataclass
class CellParameter(StaticParameter):
    cell: str


@dataclass
class DirectionParameter(DynamicParameter):
    line: int
    column: str
    number: int

    def __init__(self, line: int, column: str, number: int):
        if line < 1:
            raise ValueError(f"line must > 0 but {line}")
        if number < 1:
            raise ValueError(f'argument "number" must > 0 but {line}')

        self.line = line
        self.column = column
        self.number = number


@dataclass
class RepeatParameter(DynamicParameter):
    line: int
    column: str
    number: int

    def __init__(self, line: int, column: str, number: int):
        if line < 1:
            raise ValueError
        if number < 1:
            raise ValueError

        self.line = line
        self.column = column
        self.number = number


class ExcelReader:
    def __init__(
        self,
        fullname,
        seek_start: str,
        koseigyo: int = 1,
        sheetname: str = None,
    ):
        self.fullname = fullname
        self.seek_start = seek_start
        self.koseigyo = koseigyo
        self.sheetname = sheetname

        self.parameters = []

        self.count = 0

    def get_dataframe(self) -> pd.DataFrame():

        xw.App(visible=False)

        self._wb = xw.Book(
            self.fullname, read_only=True, ignore_read_only_recommended=True
        )

        # sheetnameが指定されていない場合は最初のシートを対象とする
        self._sht = self._wb.sheets[self.sheetname if self.sheetname else 0]

        # 読込み行数を取得
        while self._sht.range(self.seek_start).offset(row_offset=self.count).value:
            self.count += self.koseigyo

        buffer = []

        for param in self.parameters:
            # fixed, cell
            if isinstance(param, StaticParameter):

                if isinstance(param, FixedParameter):
                    ser = pd.Series([param.value] * self.count)

                elif isinstance(param, CellParameter):
                    ser = pd.Series(
                        [self._sht.range(param.cell).value] * self.count)

                buffer.append(ser)

            # direction, repeat
            elif isinstance(param, DynamicParameter):
                for j in range(param.number):

                    # 始点セルと終点セルを取得
                    r1 = self._sht.range(
                        param.column +
                        str(self._sht.range(self.seek_start).row)
                    ).offset(column_offset=j)
                    r2 = r1.offset(row_offset=self.count - 1)
                    ser = pd.Series(self._sht.range(r1, r2).value)

                    # Repeatの場合はnaを直前の値で埋める
                    if isinstance(param, RepeatParameter):
                        ser.ffill(inplace=True)

                    buffer.append(ser)

        self._wb.close()

        return pd.DataFrame({k: v for k, v in zip(range(len(buffer)), buffer)})
