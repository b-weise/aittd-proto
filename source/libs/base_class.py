import time
from dataclasses import dataclass
from functools import wraps
from pathlib import Path
from typing import Optional

import dacite

from source.libs.multi_rotating_logger import MultiRotatingLogger
from source.types.logger_types import TermLoggerType


@dataclass(kw_only=True)
class BaseConfig:
    logs_folder: Path
    short_term_logger_filename: Optional[str] = None
    short_term_logger_backup_count: Optional[int] = None
    long_term_logger_filename: Optional[str] = None
    long_term_logger_backup_count: Optional[int] = None


@dataclass(frozen=True)
class VerboseLevel:
    NONE = 0
    LOCAL = 1
    EXTENDED = 2
    DEFAULT = NONE


def base_method(method):
    @wraps(method)
    def base_method_wrapper(instance: BaseClass, *args, verbose_level: Optional[VerboseLevel] = None, **kwargs):
        if verbose_level is None:
            verbose_level = instance._default_verbose_level
        previous_dynamic_verbose_level = None
        if instance._dynamic_verbose_level != VerboseLevel.EXTENDED:
            previous_dynamic_verbose_level = instance._dynamic_verbose_level
            instance._dynamic_verbose_level = verbose_level

        if instance._dynamic_verbose_level != VerboseLevel.NONE:
            instance._logger.info(TermLoggerType.SHORT, f'Calling: {method.__name__}')
        call_time = time.time()
        output = method(instance, *args, **kwargs)
        elapsed_time = time.time() - call_time
        if instance._dynamic_verbose_level != VerboseLevel.NONE:
            instance._logger.info(TermLoggerType.SHORT, f'Exiting: {method.__name__} ({elapsed_time:.3f}s)')

        if (instance._dynamic_verbose_level != VerboseLevel.EXTENDED
                or (verbose_level == VerboseLevel.EXTENDED and previous_dynamic_verbose_level is not None)):
            instance._dynamic_verbose_level = previous_dynamic_verbose_level
        return output

    return base_method_wrapper


class BaseClass:

    def __init__(self, config_template: dataclass, config_payload: dict, default_verbose_level: Optional[VerboseLevel]):
        self._config = None
        self.__load_configs(config_template, config_payload)

        self._logger = None
        self.__initialize_logger()

        self._default_verbose_level = default_verbose_level or VerboseLevel.DEFAULT
        self._dynamic_verbose_level = self._default_verbose_level

    def __load_configs(self, config_template: dataclass, config_payload: dict):
        self._config = dacite.from_dict(config_template, config_payload)

    def __initialize_logger(self):
        def ensure_filename(filename: str, default_suffix: str):
            if filename is None or filename == '':
                default_filename = f'{self.__class__.__name__}{default_suffix}.bwpylog'
                return default_filename
            return filename

        self._config.short_term_logger_filename = ensure_filename(self._config.short_term_logger_filename,
                                                                  default_suffix='_short_term')
        self._config.long_term_logger_filename = ensure_filename(self._config.long_term_logger_filename,
                                                                 default_suffix='_long_term')

        logger_configs = {
            TermLoggerType.SHORT: {
                'rfh_filename': self._config.logs_folder / self._config.short_term_logger_filename,
            },
            TermLoggerType.LONG: {
                'rfh_filename': self._config.logs_folder / self._config.long_term_logger_filename,
            }
        }

        if self._config.short_term_logger_backup_count is not None:
            logger_configs[TermLoggerType.SHORT]['rfh_backup_count'] = self._config.short_term_logger_backup_count

        if self._config.long_term_logger_backup_count is not None:
            logger_configs[TermLoggerType.LONG]['rfh_backup_count'] = self._config.long_term_logger_backup_count

        self._logger = MultiRotatingLogger(configs=logger_configs)

    def destroy(self):
        self._logger.destroy()
