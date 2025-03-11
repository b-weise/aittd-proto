from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas
from pandas import DataFrame

from source.libs.base_class import BaseConfig, VerboseLevel, base_method, BaseClass
from source.libs.helper import Helper
from source.types.logger_types import TermLoggerType


@dataclass
class Config(BaseConfig):
    default_field_name: Optional[str] = None
    pandas_log_use_custom_settings: bool = True
    pandas_log_display_width: int = 1000
    pandas_log_display_max_cols: Optional[int] = None


class UndefinedFieldName(Exception):
    pass


class InvalidTimeRange(Exception):
    pass


class DataManager(BaseClass):

    def __init__(self,
                 config: dict,
                 dataframe: Optional[DataFrame] = None,
                 default_verbose_level: Optional[VerboseLevel] = None):
        super().__init__(Config, config, default_verbose_level)

        if dataframe is not None:
            self.__dataframe = dataframe

        if self._config.pandas_log_use_custom_settings:
            pandas.set_option('display.width', self._config.pandas_log_display_width)
            pandas.set_option('display.max_columns', self._config.pandas_log_display_max_cols)

        self._logger.info(TermLoggerType.ALL, f'{Helper.get_fully_qualified_name(self.__class__)} was initialized')

    @base_method
    def load_csv(self, path: Path) -> DataFrame:
        self.__dataframe = pandas.read_csv(path)
        if self._dynamic_verbose_level != VerboseLevel.NONE:
            self._logger.debug(TermLoggerType.SHORT, f'CSV was loaded: {path}')
            self._logger.debug(TermLoggerType.SHORT, f'Sample:\n{self.__dataframe}')
        return self.__dataframe

    @base_method
    def get_length(self) -> int:
        df_length = len(self.__dataframe)
        if self._dynamic_verbose_level != VerboseLevel.NONE:
            self._logger.debug(TermLoggerType.SHORT, f'Length: {df_length}')
        return df_length

    @base_method
    def time_filter(self,
                    field_name: Optional[str] = None,
                    time_from: Optional[datetime] = None,
                    time_to: Optional[datetime] = None,
                    count_from_start: Optional[int] = None,
                    count_to_end: Optional[int] = None,
                    ) -> DataFrame:
        if field_name is None:
            if self._config.default_field_name is None:
                raise UndefinedFieldName(
                    'Both the "field_name" parameter and the "default_field_name" option were not set. At least one of them is required.')
            else:
                field_name = self._config.default_field_name

        time_from_as_str = None
        time_from_condition = lambda: True
        if time_from is not None:
            time_from_as_str = str(time_from)
            time_from_condition = lambda: (self.__dataframe[field_name] >= time_from_as_str)

        time_to_as_str = None
        time_to_condition = lambda: True
        if time_to is not None:
            time_to_as_str = str(time_to)
            time_to_condition = lambda: (self.__dataframe[field_name] < time_to_as_str)

        if time_from is not None and time_to is not None and time_from >= time_to:
            raise InvalidTimeRange(
                f'"time_from" ({time_from_as_str}) must be earlier than "time_to" ({time_to_as_str}).')

        self.__dataframe = self.__dataframe.loc[time_from_condition() & time_to_condition()]

        if count_from_start is not None:
            self.__dataframe = self.__dataframe[:count_from_start]
        if count_to_end is not None:
            self.__dataframe = self.__dataframe[-count_to_end::]

        if self._dynamic_verbose_level != VerboseLevel.NONE:
            self._logger.debug(TermLoggerType.SHORT,
                               f'Data was filtered:\nFrom: {time_from_as_str} (Start Count: {count_from_start}) - To: {time_to_as_str} (End Count: {count_to_end})')
            self._logger.debug(TermLoggerType.SHORT, f'Sample:\n{self.__dataframe}')

        return self.__dataframe

    @base_method
    def destroy(self):
        super().destroy()
