# -*- coding: UTF-8 -*-
from dataclasses import dataclass, field
from typing import List
import pandas as pd
import xlwings as xw
import datetime


class PyMoiReader:
    """PyMoiReader is a class that reads the data from the Excel file and returns a Pandas DataFrame
    """

    def read(self):
        return pd.DataFrame()

    def export_config(self):
        return {}


class CsvReader(PyMoiReader):
    """CsvReader is a class that reads the data from the CSV file and returns a Pandas DataFrame
    """

    def __init__(self, fullname, delimiter=',', quotechar='"'):
        """initializes the CsvReader class

        Args:
            fullname ([type]): file name
            delimiter (str, optional): sets the delimiter for the CSV file. Defaults to ','.
            quotechar (str, optional): sets the quote character for the CSV file. Defaults to '"'.
        """
        self.fullname = fullname
        self.delimiter = delimiter
        self.quotechar = quotechar

    def read(self):
        """reads the data from the CSV file and returns a Pandas DataFrame

        Returns:
            pandas.DataFrame: Pandas DataFrame
        """
        df = pd.read_csv(self.fullname, delimiter=self.delimiter,
                         quotechar=self.quotechar)
        return df

    def export_config(self):
        return {
            'reader_type': 'csv',
            'fullname': self.fullname,
            'delimiter': self.delimiter,
            'quotechar': self.quotechar
        }


class ExcelReader(PyMoiReader):
    """ExcelReader is a class that reads the data from the Excel file and returns a Pandas DataFrame
    """

    def __init__(
        self,
        fullname,
        seek_start: str,
        names: list,
        unit_row: int = 1,
        sheetname: str = None,
    ):
        """initializes the ExcelReader class

        Args:
            fullname ([type]): file name
            seek_start (str): sets the cell name of the first row to be read
            names (list): sets the column names of the DataFrame
            unit_row (int, optional): sets the number of rows to be read. Defaults to 1.
            sheetname (str, optional): sets the sheet name. Defaults to None.
        """
        self.fullname = fullname
        self.seek_start = seek_start
        self.unit_row = unit_row
        self.sheetname = sheetname
        self.names = names

        self.parameters = []

        self.count = 0

    def read(self):
        """reads the data from the Excel file and returns a Pandas DataFrame
        """
        xw.App(visible=False)

        self._wb = xw.Book(
            self.fullname, read_only=True, ignore_read_only_recommended=True
        )

        # sheetnameが指定されていない場合は最初のシートを対象とする
        self._sht = self._wb.sheets[self.sheetname if self.sheetname else 0]

        # 読込み行数を取得
        while self._sht.range(self.seek_start).offset(row_offset=self.count).value:
            self.count += self.unit_row

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

        df = pd.DataFrame({k: v for k, v in zip(range(len(buffer)), buffer)})
        df.columns = self.names
        return df

    def export_config(self):
        return {
            'reader_type': 'excel',
            'fullname': self.fullname,
            'seek_start': self.seek_start,
            'unit_row': self.unit_row,
            'sheetname': self.sheetname,
            'names': self.names,
            'parameters': [param.export_config() for param in self.parameters]
        }


@dataclass
class Parameter:
    """Parameter is a class that defines the parameter of the ExcelReader class
    """

    def export_config(self):
        return {}


@dataclass
class StaticParameter(Parameter):
    """StaticParameter is a class that defines the static parameter of the ExcelReader class
    """
    pass


@dataclass
class DynamicParameter(Parameter):
    """DynamicParameter is a class that defines the dynamic parameter of the ExcelReader class
    """
    pass


@dataclass
class FixedParameter(StaticParameter):
    """FixedParameter is a class that defines the fixed parameter of the ExcelReader class
    """
    value: str

    # TODO: システム日付と本日の違いは？
    __reserved_params = {
        "#システム日時": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "#システム日付": datetime.datetime.today().strftime('%Y-%m-%d'),
        "#本日": datetime.datetime.today().strftime('%Y-%m-%d'),
    }

    def __post_init__(self):
        """after initialization, the value is replaced by the reserved value
        """
        self.value = self.__reserved_params.get(self.value, self.value)

    def export_config(self):
        return {
            'type': 'fixed',
            'value': self.value
        }

    # def __str__(self):
    #     return f"固定値:{self.value}"


@dataclass
class CellParameter(StaticParameter):
    """CellParameter is a class that defines the cell parameter of the ExcelReader class
    """
    cell: str

    def export_config(self):
        return {
            'type': 'cell',
            'cell': self.cell
        }

    # def __str__(self):
    #     return f"セル位置:{self.cell}"


@dataclass
class DirectionParameter(DynamicParameter):
    """DirectionParameter is a class that defines the direction parameter of the ExcelReader class
    """
    line: int
    column: str
    number: int

    def __init__(self, line: int, column: str, number: int):
        """initializes the DirectionParameter class

        Args:
            line (int): sets the line number
            column (str): sets the column name
            number (int): sets the number of the column

        Raises:
            ValueError: line number is must be greater than 0
            ValueError: number of the column is must be greater than 0
        """
        if line < 1:
            raise ValueError(f"line must > 0 but {line}")
        if not column:
            raise ValueError(f"column name must input")
        if number < 1:
            raise ValueError(f'argument "number" must > 0 but {number}')

        self.line = line
        self.column = column
        self.number = number

    def export_config(self):
        return {
            'type': 'direction',
            'line': self.line,
            'column': self.column,
            'number': self.number
        }


@dataclass
class RepeatParameter(DynamicParameter):
    """RepeatParameter is a class that defines the repeat parameter of the ExcelReader class
    """
    line: int
    column: str
    number: int

    def __init__(self, line: int, column: str, number: int):
        """initializes the RepeatParameter class

        Args:
            line (int): line number
            column (str): column name
            number (int): number of the column

        Raises:
            ValueError: line number is must be greater than 0
            ValueError: number of the column is must be greater than 0
        """
        if line < 1:
            raise ValueError
        if number < 1:
            raise ValueError

        self.line = line
        self.column = column
        self.number = number

    def export_config(self):
        return {
            'type': 'repeat',
            'line': self.line,
            'column': self.column,
            'number': self.number
        }
