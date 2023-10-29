#!/usr/bin/env python3

from pandas import read_sql_table
import sqlalchemy as sa
from EMS.manager import get_gbq_credentials


def copy_table_to_gbq(table_name: str):
    engine = sa.create_engine('sqlite:///data/EMS.db3', echo=False)
    ldb = engine.connect()  # ldb == Local Database. 'db' will be the remote persistent db.

    df = read_sql_table(table_name, ldb)
    df.drop(columns=['index'])
    df.to_csv(f'{table_name}.csv')

    ldb.close()
    engine.dispose()


if __name__ == "__main__":
    copy_table_to_gbq('su_ID_Example_Table_Name')
