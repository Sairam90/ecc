import pandas as pd
import json
import os
from sqlalchemy import create_engine
import logging
from datetime import datetime
from module import ecc_email

from typing import List

PROJECT_PATH = os.getcwd()
print(PROJECT_PATH)

logging.basicConfig(filename='Eod_Intrday.log',
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)

logging.getLogger().setLevel(logging.INFO)


def ingest_from_file(filename) -> pd.DataFrame:
    return pd.read_csv(os.path.join(PROJECT_PATH, f'input/{filename}'))


class DBOps():
    def __init__(self):
        self.postgres_engine, self.postgres_dbconn = self.get_result_db_conn()

    @staticmethod
    def get_result_db_conn():
        db_user = "docker"
        db_password = "docker"
        db_host = "0.0.0.0"
        db_default = "eccdb"
        postgres_engine = create_engine(
            f"postgresql://{db_user}:{db_password}@{db_host}:5432/{db_default}"
        )
        postgres_dbconn = postgres_engine.connect()
        return postgres_engine, postgres_dbconn

    def read_as_df(self, sql) -> pd.DataFrame:
        try:
            df = pd.read_sql(sql=sql, con=self.postgres_dbconn, parse_dates=True)
            return df
        except Exception as e:
            logging.info(f"Error reading data from {sql} -- ", e)
            raise e

    def write_to_table(self):
        raise NotImplementedError('To do')


class ECCPlausibilityChecks():
    def __init__(self):
        raise NotImplementedError("To do")


def check_schema(df: pd.DataFrame, validation_cols: List[str]) -> str:
    cols_in_file = list(df.columns)
    return str(cols_in_file == validation_cols)


def filter_margin_type(df: pd.DataFrame) -> pd.DataFrame:
    with open(os.path.join(PROJECT_PATH, "input", "margin_types.json")) as MarginTypeConfig:
        margin_types = json.load(MarginTypeConfig)['margin_types']

        filtered_df = df[~df['margin type'].isin(margin_types)]
        return filtered_df


def check_eod_first_intraday_values(eod_df: pd.DataFrame, first_intraday_df: pd.DataFrame) -> str:
    diff_df = eod_df.merge(first_intraday_df, how='outer', indicator=True)
    result_df = diff_df.loc[lambda x: x['_merge'] != 'both']
    if result_df.shape[0] > 0:
        msg = "Discrepancy in Eod and intraday records"
        logging.error(f"[{datetime.utcnow().strftime('%Y-%m-%d')}:] {msg}")
        logging.error(f"[{datetime.utcnow().strftime('%Y-%m-%d')}:] - Diff records - {result_df})")
        return "Discrepancy in Eod and intraday records"
    else:
        msg = "Eod and intraday records match!"
        logging.info(f"[{datetime.utcnow().strftime('%Y-%m-%d')}:] {msg}")
        return msg


if __name__ == "__main__":
    eod_df = ingest_from_file("eod.csv")
    first_intraday_df = ingest_from_file("first_intraday_report.csv")
    logging.info(f"[{datetime.utcnow().strftime('%Y-%m-%d')}:] Eod file schema validation complete, Result - " +
                 check_schema(eod_df, ['date', 'clearing member', 'account', 'margin type', 'margin']))
    logging.info(f"[{datetime.utcnow().strftime('%Y-%m-%d')}:] Eod file schema validation complete, Result -  " +
                 check_schema(first_intraday_df,
                              ['date', 'time of day', 'clearing member', 'account', 'margin type', 'margin']))

    """Remove date cols before rowise comparison"""
    eod_df.drop(["date"], axis=1, inplace=True)
    first_intraday_df.drop(["date", "time of day"], axis=1, inplace=True)

    """Filter margin types from config json"""
    eod_df = filter_margin_type(eod_df)
    first_intraday_df = filter_margin_type(first_intraday_df)

    """Row wise comparison of dataframes"""
    check_result = check_eod_first_intraday_values(eod_df, first_intraday_df)
    ecc_email.send_email(check_result)
    # db_obj = DBOps()
    # eod_df=db_obj.read_as_df("select * from eod")
    # print(eod_df.head())
