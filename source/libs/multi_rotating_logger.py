import inspect
import logging
import logging.handlers
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import dacite

from source.libs.helper import Helper


@dataclass(frozen=True)
class _NewLinePrefix:
    DEBUG: str = 'DEBUG > '
    INFO: str = 'INFO > '
    WARNING: str = 'WARNING > '
    ERROR: str = 'ERROR > '
    CRITICAL: str = 'CRITICAL > '


@dataclass
class Config:
    rfh_filename: Path
    logger_name: Optional[str] = None
    level: int = logging.DEBUG
    formatter_full_format: str = '[%(asctime)s,%(msecs)03d](%(levelname)-8s)%(message)s'
    formatter_datetime_format: str = '%y%m%d %H:%M:%S'
    custom_entry_format_string: str = '{}: {}'
    custom_entry_format_trace_separator: str = '.'
    rfh_max_bytes: int = 1024 * 1024  # 1mb
    rfh_backup_count: int = 3
    outter_internals_scope_halt_list: tuple = ('_worker', '<module>', 'main', '_run', 'wrapper_func')
    callable_names_skip_list: tuple = ('base_method_wrapper',)


class UnavailableNameException(Exception):
    pass


class MultiRotatingLogger:
    def __init__(self,
                 configs: Sequence[dict],
                 ):
        self.__configs: list[Config] = []
        if len(configs) > 0:
            self.__load_configs(configs)

        self.__loggers = []

        self.__build_loggers()

    def __load_configs(self, configs: Sequence[dict]):
        def get_sanitized_filename():
            raw_filename = configs[config_index]["rfh_filename"].stem  # last extension is removed
            sanitized_filename = Helper.sanitize_name(raw_filename)
            return sanitized_filename

        for config_index in range(len(configs)):
            configs[config_index]['logger_name'] = (
                    configs[config_index].get('logger_name', None)
                    or f'{config_index:03d}_{get_sanitized_filename()}'
            )
            self.__configs.append(dacite.from_dict(Config, configs[config_index]))

    @staticmethod
    def __check_name_availability(tentative_name: str):
        current_names_in_runtime = [name for name in logging.root.manager.loggerDict.keys()]
        if tentative_name in current_names_in_runtime:
            raise UnavailableNameException(f'A logger named \"{tentative_name}\" already exists.')

    def __build_loggers(self):
        for config_obj in self.__configs:
            self.__check_name_availability(config_obj.logger_name)

            logger = logging.getLogger(config_obj.logger_name)
            logger.setLevel(config_obj.level)
            rotating_file_handler = logging.handlers.RotatingFileHandler(filename=config_obj.rfh_filename,
                                                                         maxBytes=config_obj.rfh_max_bytes,
                                                                         backupCount=config_obj.rfh_backup_count)
            rotating_file_handler.setLevel(config_obj.level)
            formatter = logging.Formatter(config_obj.formatter_full_format,
                                          datefmt=config_obj.formatter_datetime_format)
            rotating_file_handler.setFormatter(formatter)
            logger.addHandler(rotating_file_handler)
            self.__loggers.append(logger)

    def __format_message(self, logger_index: int, message: str, prefix: str, method_name: str):
        def build_stacktrace():
            stack = inspect.stack()
            trace = ''
            logger_class_outter_level_reached = False
            has_first_stack_level_been_logged = False
            stack_function_name_index = 3
            for level in range(len(stack)):
                stack_level_name = str(stack[level][stack_function_name_index])
                if stack_level_name in self.__configs[logger_index].outter_internals_scope_halt_list:
                    # skip internals scope and beyond
                    break
                elif stack_level_name in self.__configs[logger_index].callable_names_skip_list:
                    # skip specific callables
                    continue
                elif logger_class_outter_level_reached:  # skip this class functions
                    if has_first_stack_level_been_logged:
                        trace = self.__configs[logger_index].custom_entry_format_trace_separator + trace
                    trace = stack_level_name + trace
                    has_first_stack_level_been_logged = True
                if not logger_class_outter_level_reached and stack_level_name == method_name:
                    logger_class_outter_level_reached = True
            return trace

        trace = build_stacktrace()
        entry_payload = self.__configs[logger_index].custom_entry_format_string.format(trace, message)
        entry_payload = entry_payload.replace('\n', '\n' + prefix)
        return entry_payload

    def __log_entry(self,
                    loggers_indexes: int | Sequence[int],
                    message: str,
                    method_name: str,
                    log_prefix: str,
                    one_line: bool):
        if one_line:
            message = message.replace('\n', ' ')
        if type(loggers_indexes) is int:
            loggers_indexes = [loggers_indexes]
        for logger_index in loggers_indexes:
            getattr(self.__loggers[logger_index],
                    method_name)(self.__format_message(logger_index,
                                                       message,
                                                       log_prefix,
                                                       method_name))

    def destroy(self):
        def close_handlers(logger):
            for handler in logger.handlers:
                logger.removeHandler(handler)
                handler.close()

        for logger in self.__loggers:
            close_handlers(logger)

    # -------------------------///// LOG LEVELS \\\\\-------------------------

    def debug(self, loggers_indexes: int | Sequence[int], message: str, one_line: bool = False):
        self.__log_entry(loggers_indexes, message, 'debug', _NewLinePrefix.DEBUG, one_line)

    def info(self, loggers_indexes: int | Sequence[int], message: str, one_line: bool = False):
        self.__log_entry(loggers_indexes, message, 'info', _NewLinePrefix.INFO, one_line)

    def warning(self, loggers_indexes: int | Sequence[int], message: str, one_line: bool = False):
        self.__log_entry(loggers_indexes, message, 'warning', _NewLinePrefix.WARNING, one_line)

    def error(self, loggers_indexes: int | Sequence[int], message: str, one_line: bool = False):
        self.__log_entry(loggers_indexes, message, 'error', _NewLinePrefix.ERROR, one_line)

    def critical(self, loggers_indexes: int | Sequence[int], message: str, one_line: bool = False):
        self.__log_entry(loggers_indexes, message, 'critical', _NewLinePrefix.CRITICAL, one_line)

    # -------------------------\\\\\ LOG LEVELS /////-------------------------
