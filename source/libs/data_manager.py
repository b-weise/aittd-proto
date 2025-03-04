from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas
from pandas import DataFrame

from source.libs.base_class import BaseConfig, VerboseLevel, base_method, BaseClass
from source.libs.helper import Helper
from source.types.logger_types import TermLoggerType


@dataclass
class Config(BaseConfig):
    pandas_log_use_custom_settings: bool = True
    pandas_log_display_width: int = 1000
    pandas_log_display_max_cols: Optional[int] = None


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
    def destroy(self):
        super().destroy()
