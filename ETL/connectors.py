import logging
import pandas as pd
import sqlalchemy
import yaml
import psycopg2
import psycopg2.extras
import io

from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from pyhive import presto, trino

from enum import Enum
from datetime import datetime
from collections.abc import Iterator
from typing import Dict, Tuple, Union, Type, Sequence, Iterator, Optional, Any
from abc import abstractmethod
from creds.paths_for_scripts import path_logs


LOG_PATH = path_logs


file_handler = logging.FileHandler(LOG_PATH)
stream_handler = logging.StreamHandler()
logger = logging.getLogger('connectors')
logger.addHandler(file_handler)
logger.addHandler(stream_handler)
logger.setLevel(logging.INFO)


def clean_csv_value(value: Optional[Any]) -> str:
    if value is None:
        return r'\N'
    return str(value).replace('\r', ' ').replace('^', '/').replace('\n', '\\n').replace('\\', '/')


class StringIteratorIO(io.TextIOBase):
    def __init__(self, iter: Iterator[str]):
        self._iter = iter
        self._buff = ''

    def readable(self) -> bool:
        return True

    def _read1(self, n: Optional[int] = None) -> str:
        while not self._buff:
            try:
                self._buff = next(self._iter)
            except StopIteration:
                break
        ret = self._buff[:n]
        self._buff = self._buff[len(ret):]
        return ret

    def read(self, n: Optional[int] = None) -> str:
        line = []
        if n is None or n < 0:
            while True:
                m = self._read1()
                if not m:
                    break
                line.append(m)
        else:
            while n > 0:
                m = self._read1(n)
                if not m:
                    break
                n -= len(m)
                line.append(m)
        return ''.join(line)


class TransactionStatus(Enum):
    Success = 1
    Fail = 0


class Connector:
    '''
    Base class for getting a db session and performing queries
    '''
    def __init__(self, creds: dict):
        self.creds = creds

    @abstractmethod
    def _get_engine(self) -> sqlalchemy.engine.Engine:
        return

    def _get_session(self) -> sqlalchemy.orm.Session:
        '''
        Creates an sqlalchemy session by engine

        :returns Session(): Initialized session
        '''
        engine = self._get_engine()
        Session = sessionmaker(
            expire_on_commit=False, autocommit=False, autoflush=False, bind=engine
        )
        return Session()

    def ddl_query(self, query: str) -> TransactionStatus:
        '''
        Only performs DDL queries like creating or dropping tables

        :param query: CREATE/DROP query
        :returns transaction_status: A dict with 'status' of a transaction: whether it failed or succeeded
        '''
        if not ('CREATE TABLE' in query or 'DROP TABLE' in query):
            raise Exception('Not a DDL query')
        db = self._get_session()
        try:
            db.execute(query)
        except Exception as error:
            logger.error(f'{datetime.now()},{error}')
            return TransactionStatus.Fail
        else:
            db.commit()
            logger.info(f'{datetime.now()},query performed')
            return TransactionStatus.Success

    def query(self, query: str) -> Dict[str, Union[TransactionStatus, Tuple[dict]]]:
        '''
        Only performs data manipulation (SELECT) queries

        :param query: DML query (SELECT * FROM table)
        :returns transaction_result: dict with a query's status and results tuple if received any
        '''
        if 'CREATE TABLE' in query or 'DROP TABLE' in query:
            raise Exception('Not a DML query')

        db = self._get_session()
        try:
            resultproxy = db.execute(query)
            result = tuple({column:value for column, value in rowproxy.items()} for rowproxy in resultproxy)
        except Exception as error:
            logger.error(f'{datetime.now()},{error}')
            transaction_result = {'status': TransactionStatus.Fail, 'results': None}
            return transaction_result
        else:
            logger.info(f'{datetime.now()},query performed')
            transaction_result = {'status': TransactionStatus.Success, 'results': result}
            return transaction_result


class LookupConnector(Connector):
    '''
    A class for performing queries to lookups db
    '''
    def __init__(self, creds: dict) -> None:
        super().__init__(creds=creds)

    def _get_connection(self):
        '''
        Initializes psycopg2 postgresql connection using given creds

        :returns connection: psycopg2 connection
        '''
        try:
            connection = psycopg2.connect(
                host='postgres-lookups-master.spb.play.dc',
                port=5433, # new 6432
                database='lookups',
                user=self.creds['login'],
                password=self.creds['password']
            )
        except Exception as error:
            logger.error(f'{datetime.now()},{error}')
        else:
            return connection

    def _get_table_fields(self, schema: str, table: str):
        connection = self._get_connection()
        connection.autocommit = True

        with connection.cursor() as cursor:
            cursor.execute(
            f'''SELECT *
                FROM information_schema.columns
                WHERE table_schema = '{schema}'
                AND table_name     = '{table}';'''
           )
            raw_result = cursor.fetchall()
        table_fields = [row[3] for row in raw_result]

        return table_fields

    def insert(self, schema: str, table: str, data: Union[pd.DataFrame, Iterator]):
        connection = self._get_connection()
        connection.autocommit = True
        table_fields = self._get_table_fields(schema=schema, table=table)

        if type(data) == pd.DataFrame:
            data = (row for row in data.to_dict(orient='records'))

        with connection.cursor() as cursor:
            string_iterator = StringIteratorIO(
                (
                    '^'.join(map(clean_csv_value, tuple(datum[key] for key in table_fields))) + '\n' for datum in data
                )
            )
#             cursor.copy_expert(f"COPY {schema}.{table} FROM STDIN", string_iterator)
            cursor.execute(f'SET search_path TO {schema}')
            cursor.copy_from(string_iterator, f'{table}', sep='^')

    def ddl_query(self, query: str) -> TransactionStatus:
        '''
        Only performs DDL queries like creating or dropping tables

        :param query: CREATE/DROP query
        :returns transaction_status: A dict with 'status' of a transaction: whether it failed or succeeded
        '''
        if not ('CREATE TABLE' in query or 'DROP TABLE' in query or 'ALTER' in query):
            raise Exception('Not a DDL query')
        connection = self._get_connection()
        connection.autocommit = True

        try:
            with connection.cursor() as cursor:
                cursor.execute(query)
        except Exception as error:
            logger.error(f'{datetime.now()},{error}')
            return TransactionStatus.Fail
        else:
            logger.info(f'{datetime.now()},query performed')
            return TransactionStatus.Success

    def query(self, query: str) -> Dict[str, Union[TransactionStatus, Tuple[dict]]]:
        '''
        Only performs data manipulation (SELECT) queries

        :param query: DML query (SELECT * FROM table)
        :returns transaction_result: dict with a query's status and results tuple if received any
        '''
        if 'CREATE TABLE' in query or 'DROP TABLE' in query:
            raise Exception('Not a DML query')

        connection = self._get_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
        except Exception as error:
            logger.error(f'{datetime.now()},{error}')
            transaction_result = {'status': TransactionStatus.Fail, 'results': None}
            return transaction_result
        else:
            logger.info(f'{datetime.now()},query performed')
            transaction_result = {'status': TransactionStatus.Success, 'results': result}
            return transaction_result


class PrestoConnector(Connector):
    '''
    A class for performing queries to presto
    '''
    def __init__(self, creds: dict) -> None:
        super().__init__(creds=creds)

    def _get_connection(self):
        try:
            connection = psycopg2.connect(
                host=self.creds['host'],
                port=self.creds['port'],
                username=self.creds['login'],
                password=self.creds['password'],
                catalog='callcenter',
                protocol='https'
            )
        except Exception as error:
            logger.error(f'{datetime.now()},{error}')
        else:
            return connection

    def _get_engine(self) -> sqlalchemy.engine.Engine:
        '''
        Initializes sqlalchemy presto engine using given creds

        :returns engine: sqlalchemy engine
        '''
        login = self.creds['login']
        password = self.creds['password']
        host = self.creds['host']

        presto_link = f'trino://{login}:{password}@trino.playteam.ru:8443'
        presto_params = {
            'protocol': 'https',
            'requests_kwargs': {'verify': False}
           }

        try:
            engine = sqlalchemy.create_engine(
                presto_link,
                connect_args=presto_params
            )
        except Exception as error:
            logger.error(f'{datetime.now()},{error}')
        else:
            return engine
        