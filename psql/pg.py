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
    def __init__(
        self,
        dbname=PG_DBNAME,
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
    ):
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

    def query(self, query: str) -> pd.DataFrame | None:
        """
        Execute a SQL query or multiple SQL statements separated by semicolons.

        For multiple statements, they will be executed in a single transaction.
        Only the result of the last statement will be returned (if it's a SELECT).

        Args:
            query: SQL query or multiple SQL statements separated by semicolons

        Returns:
            pandas DataFrame for SELECT queries, None for other queries
        """
        # Split the query into individual statements
        statements = [stmt.strip() for stmt in query.split(";") if stmt.strip()]

        if not statements:
            return None

        # If only one statement, use the existing behavior
        if len(statements) == 1:
            with self.conn.cursor() as cur:
                cur.execute(statements[0])

                if cur.description:
                    colnames = [desc[0] for desc in cur.description]
                    results = cur.fetchall()
                    if self.auto_commit:
                        self.conn.commit()
                    return pd.DataFrame(results, columns=colnames)
                else:
                    if self.auto_commit:
                        self.conn.commit()
                    return None

        # For multiple statements, execute them in a transaction
        result = None
        with self.conn.cursor() as cur:
            try:
                # Execute each statement
                for i, stmt in enumerate(statements):
                    if not stmt.strip():
                        continue

                    cur.execute(stmt)

                    # If this is the last statement and it returns results, capture them
                    if i == len(statements) - 1 and cur.description:
                        colnames = [desc[0] for desc in cur.description]
                        results = cur.fetchall()
                        result = pd.DataFrame(results, columns=colnames)

                # Commit the transaction if auto_commit is True
                if self.auto_commit:
                    self.conn.commit()

            except Exception as e:
                # Roll back the transaction on error
                self.conn.rollback()
                raise Exception(f"Error executing statement {i + 1}: {str(e)}")

        return result

    sql = query

    def insert_pg(
        self, df: pd.DataFrame, table_name: str, overwrite: bool = False
    ) -> None:
        """
        Insert a pandas DataFrame into a PostgreSQL table.

        Args:
            df: pandas DataFrame to insert
            table_name: name of the target table
            overwrite: if True, drop and recreate the table if it exists
        """
        if df.empty:
            return

        # Check if table exists
        table_exists = self.query(
            f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table_name}')"
        ).iloc[0, 0]

        # Handle overwrite option
        if table_exists and overwrite:
            # Get column types from DataFrame
            pg_types = self._get_pg_types(df)

            # Create column definitions
            columns = ", ".join([f"{col} {pg_types[col]}" for col in df.columns])

            # Drop and recreate table
            self.query(
                f"DROP TABLE IF EXISTS {table_name}; CREATE TABLE {table_name} ({columns});"
            )
        elif not table_exists:
            # Get column types from DataFrame
            pg_types = self._get_pg_types(df)

            # Create column definitions
            columns = ", ".join([f"{col} {pg_types[col]}" for col in df.columns])

            # Create new table
            self.query(f"CREATE TABLE {table_name} ({columns});")
        elif table_exists and not overwrite:
            # Truncate existing table
            raise Exception(
                f"Table {table_name} already exists. Use overwrite=True to drop and recreate the table."
            )

        # Insert data in batches using psycopg's execute_values
        batch_size = 999
        total_rows = len(df)

        for i in range(0, total_rows, batch_size):
            batch = df.iloc[i : i + batch_size]
            
            # 準備數據
            values = []
            for _, row in batch.iterrows():
                # 將行數據轉換為元組，處理 NaN 值
                row_values = []
                for val in row:
                    if pd.isna(val):
                        row_values.append(None)  # 使用 None 代替 "NULL"
                    else:
                        row_values.append(val)  # 直接使用原始值，psycopg 會處理轉義
                
                values.append(tuple(row_values))
            
            # 使用普通的 execute 方法進行批量插入
            with self.conn.cursor() as cur:
                try:
                    # 構建參數化查詢模板
                    placeholders = ", ".join(["%s" for _ in range(len(df.columns))])
                    insert_query = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({placeholders})"
                    
                    # 逐行插入數據
                    for row_values in values:
                        cur.execute(insert_query, row_values)
                    
                    if self.auto_commit:
                        self.conn.commit()
                except Exception as e:
                    self.conn.rollback()
                    raise Exception(f"Error inserting data: {str(e)}")

    def _get_pg_types(self, df: pd.DataFrame) -> dict:
        """
        Map pandas DataFrame dtypes to PostgreSQL data types.

        Args:
            df: pandas DataFrame

        Returns:
            Dictionary mapping column names to PostgreSQL data types
        """
        pg_types = {}

        for col, dtype in df.dtypes.items():
            if pd.api.types.is_integer_dtype(dtype):
                pg_types[col] = "INTEGER"
            elif pd.api.types.is_float_dtype(dtype):
                pg_types[col] = "DOUBLE PRECISION"
            elif pd.api.types.is_bool_dtype(dtype):
                pg_types[col] = "BOOLEAN"
            elif pd.api.types.is_datetime64_dtype(dtype):
                pg_types[col] = "TIMESTAMP"
            elif isinstance(dtype, pd.DatetimeTZDtype):
                pg_types[col] = "TIMESTAMP WITH TIME ZONE"
            else:
                pg_types[col] = "TEXT"

        return pg_types

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
    df_ = pg.query("SELECT * FROM test limit 1")
    print(df_)
    print(type(df_))
    if df_ is not None:
        print(df_.shape)
        print(df_.dtypes)
        print(df_.describe())
    else:
        print("No data returned from query")

    # print(pg.query("SELECT * FROM test1"))
    # print(pg.query("create table test1 (a integer, b text)"))


if __name__ == "__main__":
    main()
