#!/usr/bin/env python3

from pandas import read_sql_table
import sqlalchemy as sa


def copy_table_to_csv(table_name: str):
    engine = sa.create_engine('sqlite:///data/EMS.db3', echo=False)

    with engine.connect() as ldb:
        df = read_sql_table(table_name, ldb)

    # df.drop(columns=['index'], inplace=True)
    df.to_csv(f'{table_name}.csv', index=False)

    engine.dispose()


if __name__ == "__main__":
    copy_table_to_csv('su_ID_Example_Table_Name')
