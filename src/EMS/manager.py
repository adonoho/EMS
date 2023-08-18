#!/usr/bin/env python3

"""
    Donoho Lab Experiment Management System
"""


import copy
import json
import logging
import os
import random
import time
from datetime import datetime, timezone, timedelta
from math import floor
from pathlib import Path

import pandas as pd
from pandas import DataFrame
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.schema import MetaData
from sqlalchemy.exc import SQLAlchemyError
from pg8000.dbapi import Connection
from google.cloud.sql.connector import Connector
from google.oauth2 import service_account
from dask import delayed
from dask.distributed import Client, as_completed
import pandas_gbq.exceptions


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _touch_db_url(db_url: str):
    # TODO: Create any intermediate directories.
    db_path = db_url.split('sqlite:///')
    if db_path[0] != db_url:  # If the string was found …
        Path(db_path[1]).touch()


class Databases:

    def __init__(self, table_name: str,
                 remote: Engine = None,  # SQLAlchemy based systems
                 credentials: service_account.credentials = None, project_id: str = None):  # Google Big Query.
        self.results = []
        self.last_save = _now()
        self.table_name = table_name
        db_url = 'sqlite:///data/EMS.db3'
        _touch_db_url(db_url)
        self.local = create_engine(db_url, echo=False)
        self.remote = remote
        self.credentials = credentials
        self.project_id = project_id

    def _push_to_database(self):
        df = pd.concat(self.results)
        # Store remotely for flexibility.
        if self.remote is not None:
            try:
                with self.remote.connect() as rdb:
                    df.to_sql(self.table_name, rdb, if_exists='append', method='multi')
            except SQLAlchemyError as e:
                logging.error("%s", e)
        if self.credentials is not None:
            try:
                df.to_gbq(f'EMS.{self.table_name}',
                          if_exists='append',
                          progress_bar=False,
                          credentials=self.credentials)
            except pandas_gbq.exceptions.GenericGBQException as e:
                logging.error("%s", e)
        elif self.project_id is not None:
            try:
                df.to_gbq(f'EMS.{self.table_name}',
                          if_exists='append',
                          progress_bar=False,
                          project_id=self.project_id)
            except pandas_gbq.exceptions.GenericGBQException as e:
                logging.error("%s", e)

        # Store locally for durability.
        with self.local.connect() as ldb:
            df.to_sql(self.table_name, ldb, if_exists='append', method='multi')
        self.results = []

    def push(self, result: DataFrame):
        now = _now()
        self.results.append(result)
        if len(self.results) > 4095 or (now - self.last_save) > timedelta(seconds=60.0):
            self._push_to_database()
            self.last_save = now

    def final_push(self):
        if len(self.results) > 0:
            self._push_to_database()
        self.local.dispose()
        self.local = None
        self.remote = None
        self.credentials = None
        self.project_id = None

    def batch_result(self, result: DataFrame):
        self.results.append(result)

    def push_batch(self):
        now = _now()
        if len(self.results) > 4095 or (now - self.last_save) > timedelta(seconds=60.0):
            self._push_to_database()
            self.last_save = now

    def read_table(self) -> DataFrame:
        df = None
        if self.remote is not None:
            pass
        elif self.credentials is not None:
            try:
                df = pd.read_gbq(f'SELECT * FROM `EMS.{self.table_name}`', credentials=self.credentials)
            except pandas_gbq.exceptions.GenericGBQException as e:
                logging.error(f'{e}')
                df = None
        elif self.project_id is not None:
            try:
                df = pd.read_gbq(f'SELECT * FROM `EMS.{self.table_name}`', project_id=self.project_id)
            except pandas_gbq.exceptions.GenericGBQException as e:
                logging.error(f'{e}')
                df = None
        else:
            try:
                df = pd.read_sql_table(self.table_name, self.local, index_col='index')
            except ValueError:
                df = None
        return df

    def read_params(self, params: list) -> DataFrame:
        df = None
        if len(params) > 0:
            keys = ','.join(sorted(params[0].keys()))
            if self.remote is not None:
                pass
            elif self.credentials is not None:
                try:
                    df = pd.read_gbq(f'SELECT {keys} FROM `EMS.{self.table_name}`', credentials=self.credentials)
                except pandas_gbq.exceptions.GenericGBQException as e:
                    logging.error(f'{e}')
                    df = None
            elif self.project_id is not None:
                try:
                    df = pd.read_gbq(f'SELECT {keys} FROM `EMS.{self.table_name}`', project_id=self.project_id)
                except pandas_gbq.exceptions.GenericGBQException as e:
                    logging.error(f'{e}')
                    df = None
            else:
                try:
                    df = pd.read_sql_query(f'SELECT {keys} FROM {self.table_name}', self.local, index_col='index')
                except ValueError:
                    df = None
        else:
            df = self.read_table()
        return df


# The Cloud SQL Python Connector can be used along with SQLAlchemy using the
# 'creator' argument to 'create_engine'
def create_remote_connection_engine() -> Engine:
    def get_conn() -> Connection:
        connector = Connector()
        connection: Connection = connector.connect(
            os.environ["POSTGRES_CONNECTION_NAME"],
            "pg8000",
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASS"],
            db=os.environ["POSTGRES_DB"],
        )
        return connection

    engine = create_engine(
        "postgresql+pg8000://",
        creator=get_conn,
        echo=False,
        pool_pre_ping=True  # Force reestablishing the connection.
    )
    engine.dialect.description_encoding = None
    return engine


def active_remote_engine() -> (Engine, MetaData):
    remote = create_remote_connection_engine()
    metadata = MetaData()
    try:
        metadata.reflect(remote)  # Causes a DB query.
        return remote, metadata
    except SQLAlchemyError as e:
        logging.debug("%s", e)
        remote.dispose()
    return None, None


def get_gbq_credentials() -> service_account.Credentials:
    # path = '~/.config/gcloud/hs-deep-lab-donoho-ad747d94d2ec.json'  # Pandas-GBQ
    path = '~/.config/gcloud/hs-deep-lab-donoho-3d5cf4ffa2f7.json'  # Pandas-GBQ-DataSource
    expanded_path = os.path.expanduser(path)
    credentials = service_account.Credentials.from_service_account_file(expanded_path)
    return credentials


def unroll_parameters(parameters: dict) -> list:
    """
    'parameters': {
        'm': [50],
        'n': [1275, 2550, 3825],
        'mc': list(range(50)),
        'c4': linspace(0.25, 2.5, 10),
        'p': concatenate((linspace(0.02, 0.10, 9), linspace(0.15, 0.50, 8))),
        'q_type': [21],
        'd_type': [3]
        }
    """
    unrolled = []
    for key, values in parameters.items():
        next_unroll = []
        for value in values:
            roll = copy.deepcopy(unrolled)
            if len(roll) > 0:
                for param in roll:
                    param[key] = value
            else:
                roll.append({key: value})
            next_unroll.extend(roll)
        unrolled = next_unroll
    return unrolled


def update_index(index: int, df: DataFrame) -> DataFrame:
    as_list = df.index.tolist()
    for i in range(len(as_list)):
        as_list[i] = index + i
    df.index = as_list
    return df


def remove_stop_list(unrolled: list, stop: list) -> list:
    result = []
    sl = stop.copy()  # Copy the stop_list to allow it to shrink as items are found and removed.
    for param in unrolled:
        for s_param in sl:
            if len(param) == len(s_param):  # TODO: What should we do in the case of mismatched lengths?
                for k, v in s_param.items():
                    if param[k] != v:
                        break
                else:  # no_break => all (k, v) are equal. param is IN the stop_list.
                    sl.remove(s_param)
                    break
        else:  # no_break => param is NOT in the stop_list.
            result.append(param)
    return result


def timestamp() -> int:
    now = datetime.now(timezone.utc)
    return floor(now.timestamp())


def record_experiment(experiment: dict):
    table_name = experiment['table_name']
    now_ts = timestamp()

    with open(table_name + f'-{now_ts}.json', 'w') as json_file:
        json.dump(experiment, json_file, indent=4)


def unroll_experiment(experiment: dict) -> list:
    parameters = []
    if multi_res := experiment.get('multi_res', None):
        for params in multi_res:
            parameters.extend(unroll_parameters(params))
    else:
        parameters = unroll_parameters(experiment['parameters'])
    if stop_list := experiment.get('stop_list', None):
        parameters = remove_stop_list(parameters, stop_list)
    return parameters


def dedup_experiment_nested_loop(df: DataFrame, params: list) -> list:
    dedup = []
    for p in params:
        test = df.copy()
        for k, v in p.items():
            test = test.loc[test[k] == v]
            if len(test.index) == 0:
                dedup.append(p)
                break
    return dedup


def dedup_experiment_gpt(df: DataFrame, params: list) -> list:
    dedup = []
    dup = []
    for p in params:
        tdf = pd.DataFrame(p, index=[0])  # Create a temporary DataFrame from the dictionary
        merged = df.merge(tdf, how='left', indicator=True)

        if (merged['_merge'] == 'left_only').all():
            dedup.append(p)
        else:
            dup.append(p)

    return dedup


def dedup_experiment(df: DataFrame, params: list) -> list:
    dedup = []
    if len(params) > 0:
        keys = sorted(params[0].keys())
        df_values = set(tuple(row) for row in df[keys].to_numpy())

        for p in params:
            values = tuple(p[k] for k in keys)
            if values not in df_values:
                dedup.append(p)
                df_values.add(values)
    return dedup


def do_test_experiment(experiment: dict, instance: callable, client: Client,
                  remote: Engine = None, credentials: service_account.credentials = None):

    # Read the DB level parameters.
    table_name = experiment['table_name']

    db = Databases(table_name, remote, credentials)
    df = db.read_table()
    # try:
    #     df = pd.read_sql_table(table_name, db.local, index_col='index')
    # except ValueError:
    #     df = None
    # Save the experiment domain.
    record_experiment(experiment)

    # Prepare parameters.
    parameters = unroll_experiment(experiment)
    df = db.read_params(parameters)
    if df is not None and len(df.index) > 0:
        parameters = dedup_experiment(df, parameters)
        base_index = len(df.index)
    else:
        base_index = 0
    df = None  # Free up the DataFrame.
    random.shuffle(parameters)
    instance_count = len(parameters)
    logging.info(f'Number of Instances to calculate: {instance_count}')


def do_on_cluster(experiment: dict, instance: callable, client: Client,
                  remote: Engine = None, credentials: service_account.credentials = None):

    # Read the DB level parameters.
    table_name = experiment['table_name']
    db = Databases(table_name, remote, credentials)

    # Save the experiment domain.
    record_experiment(experiment)

    # Prepare parameters.
    parameters = unroll_experiment(experiment)
    df = db.read_params(parameters)
    if df is not None and len(df.index) > 0:
        parameters = dedup_experiment(df, parameters)
        base_index = len(df.index)
    else:
        base_index = 0
    df = None  # Free up the DataFrame.
    random.shuffle(parameters)
    instance_count = len(parameters)
    logging.info(f'Number of Instances to calculate: {instance_count}')

    # Start the computation.
    tick = time.perf_counter()
    # delayed_instance = delayed(instance)
    # futures = client.compute([delayed_instance(**p) for p in parameters])
    futures = client.map(lambda p: instance(**p), parameters)  # Properly isolates the instance keywords from `client.map()`.
    i = base_index
    for batch in as_completed(futures, with_results=True).batches():
        for future, result in batch:
            i += 1
            if not (i % 10):  # Log results every tenth output
                tock = time.perf_counter() - tick
                count = i - base_index
                s_i = tock / count
                logging.info(f"Count: {count}; Time: {round(tock)}; Seconds/Instance: {s_i:0.4f}; Remaining: {round((instance_count - count) * s_i)}")
                logging.info(result)
            db.batch_result(result)
            future.release()  # As these are Embarrassingly Parallel tasks, clean up memory.
        db.push_batch()
    db.final_push()
    total_time = time.perf_counter() - tick
    logging.info(f"Performed experiment in {total_time:0.4f} seconds")
    if instance_count > 0:
        logging.info(f"Seconds/Instance: {(total_time / instance_count):0.4f}")
    logging.info(f'Starting index: {base_index}, Count: {instance_count}, Next index: {base_index + instance_count}.')
