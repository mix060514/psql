import os

import psycopg as pg

import pandas as pd

from dotenv import load_dotenv

load_dotenv()
PG_HOST = os.environ["PG_HOST"]
PG_PORT = os.environ["PG_PORT"]
PG_DBNAME = os.environ["PG_DBNAME"]
PG_USER = os.environ["PG_USER"]
PG_PASSWORD = os.environ["PG_PASSWORD"]


class PG:
    def __init__(self, dbname=PG_DBNAME, host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self._conn = None
        self.auto_commit = True

    @property
    def conn(self) -> pg.Connection:
        if not self._conn or self._conn.closed:
            self._conn = self.connect()
        return self._conn

    def connect(self) -> pg.Connection:
        return pg.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password,
        )

    def query(self, query) -> pd.DataFrame | None:
        with self.conn.cursor() as cur:
            cur.execute(query)
            
            if cur.description:
                colnames = [desc[0] for desc in cur.description]
                results = cur.fetchall()
                return pd.DataFrame(results, columns=colnames)
            else:
                # If no description, it means it's a non-select query
                if self.auto_commit:
                    self.conn.commit()
                return None
            # return cur.fetchall()

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None
    
    def __del__(self):
        if self.auto_commit and self._conn:
            self._conn.commit()
        self.close()


def main():
    print("Hello from psql!")
    pg = PG()
    df_ = pg.query("SELECT * FROM test limit 2")
    print(df_)
    print(type(df_))
    if df_ is not None:
        print(df_.shape)
        print(df_.dtypes)
        print(df_.describe())
    else:
        print("No data returned from query")

    # print(pg.query("SELECT * FROM test2"))
    # print(pg.query("create table test2 (a integer, b text)"))


if __name__ == "__main__":
    main()
