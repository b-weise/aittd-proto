from collections.abc import Sequence
from dataclasses import dataclass
from typing import Optional

import pandas
from sqlalchemy import create_engine, BinaryExpression
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import URL
from sqlalchemy.orm import declarative_base, sessionmaker, InstrumentedAttribute

from source.libs.base_class import BaseConfig, VerboseLevel, base_method, BaseClass
from source.libs.helper import Helper
from source.types.logger_types import TermLoggerType


@dataclass
class Config(BaseConfig):
    conn_username: str
    conn_dbname: str
    conn_host: str = 'localhost'
    conn_drivername: str = 'postgresql'
    record_autofill_field_names: Sequence[str] = ('ID', 'CreatedOn', 'UpdatedOn')


class RecordsMismatchException(Exception):
    pass


class DBManager(BaseClass):
    Base = declarative_base()

    def __init__(self, config: dict, default_verbose_level: Optional[VerboseLevel] = None):
        super().__init__(Config, config, default_verbose_level)
        self.__initialize_connection()

    @base_method
    def __initialize_connection(self):
        engine_create_params = {'drivername': self._config.conn_drivername,
                                'username': self._config.conn_username,
                                'host': self._config.conn_host,
                                'database': self._config.conn_dbname}

        if self._dynamic_verbose_level != VerboseLevel.NONE:
            self._logger.debug(TermLoggerType.SHORT,
                               f'engine_create_params:\n{Helper.beautify_json(engine_create_params)}')

        self.__url = URL.create(**engine_create_params)

        self.__metadata = DBManager.Base.metadata
        self.__engine = create_engine(self.__url)
        self.__session = sessionmaker(self.__engine)

        self.create_all_tables()
        self._logger.info(TermLoggerType.ALL, f'{Helper.get_fully_qualified_name(self.__class__)} was initialized')

    @base_method
    def create_all_tables(self):
        DBManager.Base.metadata.create_all(bind=self.__engine)

    @base_method
    def drop_all_tables(self):
        DBManager.Base.metadata.drop_all(bind=self.__engine)

    @base_method
    def print_tables_names(self):
        self._logger.debug(TermLoggerType.ALL, 'Tables: {}'.format(', '.join(self.__metadata.tables.keys())))

    @base_method
    def insert(self, records: Sequence[Base]):
        def record_as_dict(record):
            record_dict = {col.name: getattr(record, col.name)
                           for col in record.__table__.columns}
            for autofill_field_name in self._config.record_autofill_field_names:
                if autofill_field_name in record_dict:
                    del record_dict[autofill_field_name]
            return record_dict

        if len(records) == 0:
            return

        table = records[0].__class__
        if not Helper.type_check_contents(values=records, expected_type=table):
            raise RecordsMismatchException('Not all records are for the same table.')

        with self.__session.begin() as session:
            record_dicts = list(map(record_as_dict, records))
            if self._dynamic_verbose_level != VerboseLevel.NONE:
                self._logger.debug(TermLoggerType.SHORT, f'table: {Helper.get_fully_qualified_name(table)}')
                self._logger.debug(TermLoggerType.SHORT, f'record_dicts:\n{Helper.beautify_json(record_dicts)}')
            insert_stmt = insert(table).values(record_dicts)
            ignore_duplicates_stmt = insert_stmt.on_conflict_do_nothing()
            session.execute(ignore_duplicates_stmt)
            session.commit()

    @base_method
    def get_columns(self,
                    columns: Sequence[InstrumentedAttribute],
                    filter_criterion: Optional[Sequence[BinaryExpression | bool]] = None) -> pandas.DataFrame:
        with self.__session.begin() as session:
            query_result = session.query(*columns)
            if filter_criterion is not None:
                query_result = query_result.filter(*filter_criterion)
            pandas_result = pandas.read_sql(sql=query_result.statement, con=self.__engine)
            if self._dynamic_verbose_level != VerboseLevel.NONE:
                self._logger.debug(TermLoggerType.SHORT, f'pandas_result:\n{str(pandas_result)}')
            return pandas_result

    @base_method
    def destroy(self):
        super().destroy()
        self.__engine.dispose()
