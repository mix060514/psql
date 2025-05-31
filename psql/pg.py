import os
import re
from typing import Optional, Tuple

import psycopg as pg
import pandas as pd

from dotenv import load_dotenv

_current_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(_current_dir, ".env"))

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

    def _parse_table_name(self, table_name: str) -> Tuple[str, str]:
        """
        解析表格名稱，提取 schema 和 table 名稱
        
        Args:
            table_name: 表格名稱，格式可以是 'table' 或 'schema.table'
            
        Returns:
            (schema_name, table_name) 的元組
        """
        # 使用正則表達式來更安全地解析，處理可能的引號
        parts = table_name.split('.')
        if len(parts) == 2:
            schema_name = parts[0].strip('"')
            table_name = parts[1].strip('"')
        elif len(parts) == 1:
            schema_name = 'public'
            table_name = parts[0].strip('"')
        else:
            raise ValueError(f"無效的表格名稱格式: {table_name}")
        
        return schema_name, table_name

    def _escape_identifier(self, identifier: str) -> str:
        """
        轉義 PostgreSQL 識別符（表格名、列名等）
        
        Args:
            identifier: 要轉義的識別符
            
        Returns:
            轉義後的識別符
        """
        # 如果識別符包含特殊字符或是保留字，需要用雙引號包圍
        if re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', identifier) and identifier.lower() not in [
            'select', 'from', 'where', 'insert', 'update', 'delete', 'create', 'drop', 'alter'
        ]:
            return identifier
        else:
            return f'"{identifier}"'

    # === Schema 管理功能 ===
    
    def create_schema(self, schema_name: str) -> None:
        """
        創建新的 schema
        
        Args:
            schema_name: schema 的名稱
        """
        escaped_schema = self._escape_identifier(schema_name)
        self.query(f"CREATE SCHEMA IF NOT EXISTS {escaped_schema};")

    def list_schemas(self) -> pd.DataFrame:
        """
        列出所有可用的 schemas
        
        Returns:
            包含所有 schema 信息的 DataFrame
        """
        return self.query("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
            ORDER BY schema_name;
        """)

    def drop_schema(self, schema_name: str, cascade: bool = False) -> None:
        """
        刪除指定的 schema
        
        Args:
            schema_name: 要刪除的 schema 名稱
            cascade: 如果為 True，會同時刪除該 schema 下的所有對象
        """
        escaped_schema = self._escape_identifier(schema_name)
        cascade_str = "CASCADE" if cascade else "RESTRICT"
        self.query(f"DROP SCHEMA IF EXISTS {escaped_schema} {cascade_str};")

    def schema_exists(self, schema_name: str) -> bool:
        """
        檢查 schema 是否存在
        
        Args:
            schema_name: schema 名稱
            
        Returns:
            True 如果 schema 存在，否則 False
        """
        result = self.query(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.schemata 
                WHERE schema_name = '{schema_name}'
            )
        """)
        return result.iloc[0, 0] if result is not None else False

    # === 表格管理功能 ===
    
    def list_tables(self, schema_name: str = 'public') -> pd.DataFrame:
        """
        列出指定 schema 中的所有表格
        
        Args:
            schema_name: schema 名稱
            
        Returns:
            包含表格信息的 DataFrame
        """
        return self.query(f"""
            SELECT table_name, table_type
            FROM information_schema.tables
            WHERE table_schema = '{schema_name}'
            ORDER BY table_name;
        """)

    def describe_table(self, table_name: str, schema_name: Optional[str] = None) -> pd.DataFrame:
        """
        獲取表格的詳細信息
        
        Args:
            table_name: 表格名稱，可以是 'table' 或 'schema.table' 格式
            schema_name: schema 名稱（如果 table_name 中已包含則忽略此參數）
            
        Returns:
            包含列信息的 DataFrame
        """
        if schema_name is None:
            schema_name, table_name = self._parse_table_name(table_name)
        
        return self.query(f"""
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default,
                character_maximum_length,
                numeric_precision,
                numeric_scale
            FROM information_schema.columns
            WHERE table_schema = '{schema_name}'
            AND table_name = '{table_name}'
            ORDER BY ordinal_position;
        """)

    def table_exists(self, table_name: str, schema_name: Optional[str] = None) -> bool:
        """
        檢查表格是否存在
        
        Args:
            table_name: 表格名稱，可以是 'table' 或 'schema.table' 格式
            schema_name: schema 名稱（如果 table_name 中已包含則忽略此參數）
            
        Returns:
            True 如果表格存在，否則 False
        """
        if schema_name is None:
            schema_name, table_name = self._parse_table_name(table_name)
        
        result = self.query(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = '{schema_name}' 
                AND table_name = '{table_name}'
            )
        """)
        return result.iloc[0, 0] if result is not None else False

    # === 原有功能（已增強）===

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
        將 pandas DataFrame 插入到 PostgreSQL 表格中
        
        Args:
            df: 要插入的 pandas DataFrame
            table_name: 目標表格名稱，格式可以是：
                       - 'my_table' (使用預設 schema 'public')
                       - 'my_schema.my_table' (指定 schema)
            overwrite: 如果為 True，會刪除並重新創建表格
        """
        if df.empty:
            return

        # 解析表格名稱
        schema_name, parsed_table_name = self._parse_table_name(table_name)
        
        # 確保 schema 存在
        if not self.schema_exists(schema_name):
            self.create_schema(schema_name)

        # 檢查表格是否存在
        table_exists = self.table_exists(parsed_table_name, schema_name)
        
        # 構建完整的表格名稱
        escaped_schema = self._escape_identifier(schema_name)
        escaped_table = self._escape_identifier(parsed_table_name)
        full_table_name = f"{escaped_schema}.{escaped_table}"

        # 處理表格創建/覆蓋邏輯
        if table_exists and overwrite:
            # 獲取列類型並重新創建表格
            pg_types = self._get_pg_types(df)
            columns = ", ".join([
                f"{self._escape_identifier(col)} {pg_types[col]}" 
                for col in df.columns
            ])
            
            self.query(f"""
                DROP TABLE IF EXISTS {full_table_name};
                CREATE TABLE {full_table_name} ({columns});
            """)
        elif not table_exists:
            # 創建新表格
            pg_types = self._get_pg_types(df)
            columns = ", ".join([
                f"{self._escape_identifier(col)} {pg_types[col]}" 
                for col in df.columns
            ])
            self.query(f"CREATE TABLE {full_table_name} ({columns});")
        elif table_exists and not overwrite:
            # 表格已存在且不覆蓋，則清空表格
            self.query(f"TRUNCATE TABLE {full_table_name};")

        # 批量插入數據
        self._insert_dataframe_batch(df, full_table_name)

    def _insert_dataframe_batch(self, df: pd.DataFrame, full_table_name: str) -> None:
        """
        批量插入 DataFrame 數據到指定表格
        
        Args:
            df: 要插入的 DataFrame
            full_table_name: 完整的表格名稱（包含 schema）
        """
        batch_size = 1000
        total_rows = len(df)

        # 準備列名（轉義）
        escaped_columns = [self._escape_identifier(col) for col in df.columns]
        columns_str = ", ".join(escaped_columns)
        
        for i in range(0, total_rows, batch_size):
            batch = df.iloc[i : i + batch_size]
            
            # 準備數據
            values = []
            for _, row in batch.iterrows():
                row_values = []
                for val in row:
                    if pd.isna(val):
                        row_values.append(None)
                    else:
                        row_values.append(val)
                values.append(tuple(row_values))
            
            # 執行批量插入
            with self.conn.cursor() as cur:
                try:
                    # 構建參數化查詢
                    placeholders = ", ".join(["%s" for _ in range(len(df.columns))])
                    insert_query = f"INSERT INTO {full_table_name} ({columns_str}) VALUES ({placeholders})"
                    
                    # 使用 executemany 進行批量插入（更高效）
                    cur.executemany(insert_query, values)
                    
                    if self.auto_commit:
                        self.conn.commit()
                except Exception as e:
                    self.conn.rollback()
                    raise Exception(f"Error inserting batch {i//batch_size + 1}: {str(e)}")

    def _get_pg_types(self, df: pd.DataFrame) -> dict:
        """
        將 pandas DataFrame 的數據類型映射到 PostgreSQL 數據類型
        
        Args:
            df: pandas DataFrame
            
        Returns:
            列名到 PostgreSQL 數據類型的字典映射
        """
        pg_types = {}

        for col, dtype in df.dtypes.items():
            if pd.api.types.is_integer_dtype(dtype):
                # 根據整數大小選擇適當的類型
                if df[col].min() >= -2147483648 and df[col].max() <= 2147483647:
                    pg_types[col] = "INTEGER"
                else:
                    pg_types[col] = "BIGINT"
            elif pd.api.types.is_float_dtype(dtype):
                pg_types[col] = "DOUBLE PRECISION"
            elif pd.api.types.is_bool_dtype(dtype):
                pg_types[col] = "BOOLEAN"
            elif pd.api.types.is_datetime64_dtype(dtype):
                pg_types[col] = "TIMESTAMP"
            elif isinstance(dtype, pd.DatetimeTZDtype):
                pg_types[col] = "TIMESTAMP WITH TIME ZONE"
            elif pd.api.types.is_categorical_dtype(dtype):
                pg_types[col] = "TEXT"
            else:
                # 對於字符串類型，嘗試估算長度
                if pd.api.types.is_string_dtype(dtype):
                    max_length = df[col].astype(str).str.len().max()
                    if pd.isna(max_length) or max_length <= 255:
                        pg_types[col] = "VARCHAR(255)"
                    else:
                        pg_types[col] = "TEXT"
                else:
                    pg_types[col] = "TEXT"

        return pg_types

    # === 原有功能保持不變 ===
    
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
    
    # 測試基本查詢
    df_ = pg.query("SELECT * FROM test limit 1")
    print(df_)
    print(type(df_))
    if df_ is not None:
        print(df_.shape)
        print(df_.dtypes)
        print(df_.describe())
    else:
        print("No data returned from query")

    # 測試 schema 功能
    try:
        print("\n=== Schema 管理測試 ===")
        schemas = pg.list_schemas()
        print("現有 Schemas:")
        print(schemas)
        
        # 創建測試 schema
        pg.create_schema('test_schema')
        print("已創建 test_schema")
        
        # 測試 DataFrame 插入
        print("\n=== DataFrame 插入測試 ===")
        import pandas as pd
        test_df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['張三', '李四', '王五'],
            'age': [25, 30, 35],
            'salary': [50000.5, 60000.0, 55000.75]
        })
        
        # 插入到指定 schema
        pg.insert_pg(test_df, 'test_schema.employees', overwrite=True)
        print("已插入數據到 test_schema.employees")
        
        # 查詢插入的數據
        result = pg.query("SELECT * FROM test_schema.employees")
        print("查詢結果:")
        print(result)
        
        # 查看表格結構
        table_info = pg.describe_table('test_schema.employees')
        print("\n表格結構:")
        print(table_info)
        
    except Exception as e:
        print(f"測試過程中發生錯誤: {e}")


if __name__ == "__main__":
    main()
