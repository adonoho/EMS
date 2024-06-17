#!/usr/bin/env python3

from pathlib import Path
import json
import argparse
import sys

from EMS.manager import get_gbq_credentials, read_json, dedup_experiment_from_db


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--exp", help="Experiment JSON", type=str, default="exp", required=True)

    exp_name = parser.parse_args().exp
    exp_path = Path(exp_name)
    exp_path.resolve()
    exp = read_json(exp_path)

    params = dedup_experiment_from_db(exp, credentials=get_gbq_credentials())

    json.dump(params, sys.stdout, indent=4)
