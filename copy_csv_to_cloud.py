#!/usr/bin/env python3

from pathlib import Path
from pandas import read_csv, DataFrame
from pandas_gbq import to_gbq
from EMS.manager import get_gbq_credentials
import argparse


def copy_df_to_gbq(df: DataFrame, table_name: str):
    to_gbq(df, f'EMS.{table_name}',
           if_exists='append',
           chunksize=1000,
           progress_bar=False,
           credentials=get_gbq_credentials())


def copy_csv_to_table_on_gbq(filename: str, table_name: str):
    p = Path(filename)
    p.resolve()
    df = read_csv(p, names=['SUID', 'ensemble', 'n_idx', 'N', 'k_idx', 'NMonte', 'v_1', 'v_2', 'v_3', 'dT'])
    copy_df_to_gbq(df, table_name)
    pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--table", help="GBQ Table Name", type=str, default="table", required=True)
    parser.add_argument("-f", "--file", help="CSV Filename", type=str, default="file.csv", required=True)
    args = parser.parse_args()

    copy_csv_to_table_on_gbq(args.file, args.table)
