import pytest
import pandas as pd
import numpy as np

from psql.PG import PG


def test_query():
    pg = PG()
    df_ = pg.query("SELECT * FROM test limit 2")
    assert isinstance(df_, pd.DataFrame)


def test_multiple_statements():
    """Test executing multiple SQL statements in a single query."""
    pg = PG()

    # Create a temporary test table
    result = pg.query("""
        DROP TABLE IF EXISTS test_multi;
        CREATE TABLE test_multi (id INTEGER, name TEXT);
        INSERT INTO test_multi VALUES (1, 'Alice'), (2, 'Bob');
        SELECT * FROM test_multi ORDER BY id;
    """)

    # Check that we got results from the last statement
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert list(result.columns) == ["id", "name"]
    assert result["id"].tolist() == [1, 2]
    assert result["name"].tolist() == ["Alice", "Bob"]

    # Clean up
    pg.query("DROP TABLE IF EXISTS test_multi;")


def test_insert_pg():
    """Test inserting a pandas DataFrame into a PostgreSQL table."""
    pg = PG()

    # Create a test DataFrame
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "age": [25, 30, 35],
            "active": [True, False, True],
            "score": [92.5, 88.0, 95.5],
            "created_at": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"]),
        }
    )

    # Test creating a new table
    pg.insert_pg(df, "test_insert", overwrite=True)

    # Query the inserted data
    result = pg.query("SELECT * FROM test_insert ORDER BY id")

    # Check the results
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 3
    assert list(result.columns) == [
        "id",
        "name",
        "age",
        "active",
        "score",
        "created_at",
    ]
    assert result["id"].tolist() == [1, 2, 3]
    assert result["name"].tolist() == ["Alice", "Bob", "Charlie"]

    # Test overwriting existing table
    df2 = pd.DataFrame(
        {
            "id": [4, 5],
            "name": ["Dave", "Eve"],
            "age": [40, 45],
            "active": [False, True],
            "score": [78.5, 82.0],
            "created_at": pd.to_datetime(["2023-01-04", "2023-01-05"]),
        }
    )

    pg.insert_pg(df2, "test_insert", overwrite=True)

    # Query the new data
    result = pg.query("SELECT * FROM test_insert ORDER BY id")

    # Check the results
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert result["id"].tolist() == [4, 5]
    assert result["name"].tolist() == ["Dave", "Eve"]

    # Clean up
    pg.query("DROP TABLE IF EXISTS test_insert;")


def test_insert_pg_special_chars():
    """Test inserting a pandas DataFrame with special characters into a PostgreSQL table."""
    pg = PG()

    # 創建一個包含各種特殊字符的測試 DataFrame
    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5, 6],
            "single_quote": ["It's a test", "Don't worry", "O'Reilly", "Let's go", "That's fine", "Isn't it?"],
            "double_quote": ['He said "hello"', 'The "best" way', '"Quoted" text', 'Say "cheese"', '"Start" and "end"', 'Multiple "quotes" here'],
            "newlines": ["First line\nSecond line", "Another\nline", "Multi\nline\ntext", "Line1\r\nLine2", "Text with\nnewlines", "More\r\nlines\nhere"],
            "special_chars": ["Back\\slash", "Tab\tcharacter", "Percent%sign", "Ampersand&symbol", "Hash#tag", "At@symbol"],
            "mixed_chars": ["It's a \"mixed\" text\nwith newline", "Special\\chars\tand\rreturns", "Both 'single' and \"double\"\nquotes", "Tabs\tand\tnewlines\n", "Slashes / \\ and quotes ' \"", "Mix of %$#@!^&*()"],
            "sql_injection": ["'; DROP TABLE test; --", "'); DELETE FROM users; --", "OR 1=1; --", "UNION SELECT * FROM passwords; --", "1'; UPDATE users SET admin=true; --", "Robert'); DROP TABLE students; --"],
        }
    )

    # 測試創建新表並插入特殊字符數據
    pg.insert_pg(df, "test_special_chars", overwrite=True)

    # 查詢插入的數據
    result = pg.query("SELECT * FROM test_special_chars ORDER BY id")

    # 檢查結果
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 6
    
    # 驗證單引號字符串
    assert result["single_quote"].tolist() == df["single_quote"].tolist()
    
    # 驗證雙引號字符串
    assert result["double_quote"].tolist() == df["double_quote"].tolist()
    
    # 驗證換行符字符串
    assert result["newlines"].tolist() == df["newlines"].tolist()
    
    # 驗證其他特殊字符字符串
    assert result["special_chars"].tolist() == df["special_chars"].tolist()
    
    # 驗證混合特殊字符字符串
    assert result["mixed_chars"].tolist() == df["mixed_chars"].tolist()
    
    # 驗證 SQL 注入嘗試字符串
    assert result["sql_injection"].tolist() == df["sql_injection"].tolist()

    # 清理
    pg.query("DROP TABLE IF EXISTS test_special_chars;")


def test_insert_pg_special_chars_batch_processing():
    """Test inserting a large DataFrame with special characters to verify batch processing."""
    pg = PG()

    # 創建一個大型 DataFrame，確保它會觸發批處理邏輯（超過1000行）
    # 我們使用較小的數據集重複多次來創建大型數據集
    base_df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "text_with_quotes": ["Single quote: ' and double quote: \"", "More 'quotes' and \"quotes\"", "Mix of 'single' and \"double\""],
            "text_with_special": ["Newline\nand tab\t", "Backslash \\ and slash /", "Special chars: !@#$%^&*()"],
        }
    )
    
    # 重複數據以創建大型 DataFrame（超過1000行以觸發批處理）
    rows = []
    for i in range(400):  # 400 * 3 = 1200 行
        for _, row in base_df.iterrows():
            new_row = row.copy()
            new_row["id"] = row["id"] + i * 3
            rows.append(new_row)
    
    large_df = pd.DataFrame(rows)
    
    # 測試創建新表並插入特殊字符數據
    pg.insert_pg(large_df, "test_special_chars_batch", overwrite=True)

    # 查詢插入的數據（僅檢查部分數據以驗證）
    result = pg.query("SELECT * FROM test_special_chars_batch ORDER BY id LIMIT 10")

    # 檢查結果
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 10
    
    # 驗證前10行數據是否正確
    for i in range(10):
        original_idx = i % 3
        batch_idx = i // 3
        expected_id = base_df.iloc[original_idx]["id"] + batch_idx * 3
        assert result.iloc[i]["id"] == expected_id
        assert result.iloc[i]["text_with_quotes"] == base_df.iloc[original_idx]["text_with_quotes"]
        assert result.iloc[i]["text_with_special"] == base_df.iloc[original_idx]["text_with_special"]

    # 檢查總行數
    count_result = pg.query("SELECT COUNT(*) FROM test_special_chars_batch")
    assert count_result.iloc[0, 0] == 1200  # 400 * 3 = 1200 行
    
    # 清理
    pg.query("DROP TABLE IF EXISTS test_special_chars_batch;")


def test_query_edge_cases():
    """Test edge cases for the query method."""
    pg = PG()

    # Test empty query
    result = pg.query("")
    assert result is None

    # Test query with only whitespace
    result = pg.query("   \n\t  ")
    assert result is None

    # Test query with only semicolons
    result = pg.query(";;;")
    assert result is None

    # Test query with empty statements between semicolons
    result = pg.query("SELECT 1; ; ; SELECT 2;")
    assert isinstance(result, pd.DataFrame)
    assert result.iloc[0, 0] == 2

    # Test single statement without semicolon
    result = pg.query("SELECT 1 as test_col")
    assert isinstance(result, pd.DataFrame)
    assert result.iloc[0, 0] == 1
    assert result.columns[0] == "test_col"

    # Test DDL statement (no result)
    result = pg.query("DROP TABLE IF EXISTS test_edge_case")
    assert result is None


def test_query_transaction_rollback():
    """Test that transactions are properly rolled back on error."""
    pg = PG()

    # Create a test table
    pg.query("DROP TABLE IF EXISTS test_rollback; CREATE TABLE test_rollback (id INTEGER);")

    # Insert initial data
    pg.query("INSERT INTO test_rollback VALUES (1);")

    # Verify initial data
    result = pg.query("SELECT COUNT(*) FROM test_rollback")
    assert result.iloc[0, 0] == 1

    # Try to execute multiple statements with an error in the middle
    try:
        pg.query("""
            INSERT INTO test_rollback VALUES (2);
            INSERT INTO test_rollback VALUES (3);
            INSERT INTO non_existent_table VALUES (4);
            INSERT INTO test_rollback VALUES (5);
        """)
        assert False, "Expected an exception"
    except Exception as e:
        assert "non_existent_table" in str(e).lower() or "does not exist" in str(e).lower()

    # Verify that the transaction was rolled back (should still have only 1 row)
    result = pg.query("SELECT COUNT(*) FROM test_rollback")
    assert result.iloc[0, 0] == 1

    # Clean up
    pg.query("DROP TABLE IF EXISTS test_rollback;")


def test_insert_pg_edge_cases():
    """Test edge cases for the insert_pg method."""
    pg = PG()

    # Test empty DataFrame
    empty_df = pd.DataFrame()
    pg.insert_pg(empty_df, "test_empty")  # Should not raise an error

    # Test DataFrame with only NaN values
    nan_df = pd.DataFrame({
        "col1": [np.nan, np.nan],
        "col2": [np.nan, np.nan]
    })
    pg.insert_pg(nan_df, "test_nan", overwrite=True)
    result = pg.query("SELECT * FROM test_nan")
    assert len(result) == 2
    assert pd.isna(result.iloc[0, 0])
    assert pd.isna(result.iloc[0, 1])

    # Test DataFrame with mixed data types
    mixed_df = pd.DataFrame({
        "int_col": [1, 2, 3],
        "float_col": [1.1, 2.2, np.nan],
        "str_col": ["a", "b", None],
        "bool_col": [True, False, None],
        "datetime_col": [pd.Timestamp("2023-01-01"), pd.Timestamp("2023-01-02"), pd.NaT]
    })
    pg.insert_pg(mixed_df, "test_mixed", overwrite=True)
    result = pg.query("SELECT * FROM test_mixed ORDER BY int_col")
    assert len(result) == 3
    assert result["int_col"].tolist() == [1, 2, 3]

    # Test table already exists without overwrite
    try:
        pg.insert_pg(mixed_df, "test_mixed", overwrite=False)
        assert False, "Expected an exception"
    except Exception as e:
        assert "already exists" in str(e)

    # Test very long table name (edge case)
    long_name = "a" * 63  # PostgreSQL identifier limit
    pg.insert_pg(pd.DataFrame({"col": [1]}), long_name, overwrite=True)
    result = pg.query(f"SELECT * FROM {long_name}")
    assert len(result) == 1

    # Clean up
    pg.query("DROP TABLE IF EXISTS test_nan;")
    pg.query("DROP TABLE IF EXISTS test_mixed;")
    pg.query(f"DROP TABLE IF EXISTS {long_name};")


def test_insert_pg_large_batch():
    """Test inserting a large DataFrame to verify batch processing works correctly."""
    pg = PG()

    # Create a DataFrame larger than batch_size (999)
    large_size = 2500
    large_df = pd.DataFrame({
        "id": range(large_size),
        "value": [f"value_{i}" for i in range(large_size)],
        "number": [i * 1.5 for i in range(large_size)]
    })

    # Insert the large DataFrame
    pg.insert_pg(large_df, "test_large_batch", overwrite=True)

    # Verify all data was inserted
    count_result = pg.query("SELECT COUNT(*) FROM test_large_batch")
    assert count_result.iloc[0, 0] == large_size

    # Verify some sample data
    sample_result = pg.query("SELECT * FROM test_large_batch WHERE id IN (0, 999, 1999, 2499) ORDER BY id")
    assert len(sample_result) == 4
    assert sample_result["id"].tolist() == [0, 999, 1999, 2499]
    assert sample_result["value"].tolist() == ["value_0", "value_999", "value_1999", "value_2499"]

    # Clean up
    pg.query("DROP TABLE IF EXISTS test_large_batch;")


def test_data_type_mapping():
    """Test that pandas data types are correctly mapped to PostgreSQL types."""
    pg = PG()

    # Create DataFrame with various data types
    df = pd.DataFrame({
        "int8_col": pd.array([1, 2, 3], dtype="int8"),
        "int16_col": pd.array([1, 2, 3], dtype="int16"),
        "int32_col": pd.array([1, 2, 3], dtype="int32"),
        "int64_col": pd.array([1, 2, 3], dtype="int64"),
        "float32_col": pd.array([1.1, 2.2, 3.3], dtype="float32"),
        "float64_col": pd.array([1.1, 2.2, 3.3], dtype="float64"),
        "bool_col": pd.array([True, False, True], dtype="bool"),
        "string_col": pd.array(["a", "b", "c"], dtype="string"),
        "object_col": ["x", "y", "z"],
        "datetime_col": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"]),
        "datetime_tz_col": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"]).tz_localize("UTC")
    })

    # Insert and verify
    pg.insert_pg(df, "test_data_types", overwrite=True)

    # Check that table was created successfully
    result = pg.query("SELECT * FROM test_data_types ORDER BY int8_col")
    assert len(result) == 3

    # Verify data integrity
    assert result["int8_col"].tolist() == [1, 2, 3]
    assert result["bool_col"].tolist() == [True, False, True]
    assert result["string_col"].tolist() == ["a", "b", "c"]

    # Clean up
    pg.query("DROP TABLE IF EXISTS test_data_types;")


def test_connection_management():
    """Test connection management and auto-reconnect functionality."""
    pg = PG()

    # Test initial connection
    assert pg._conn is None
    
    # First query should establish connection
    result = pg.query("SELECT 1")
    assert pg._conn is not None
    assert not pg._conn.closed

    # Manually close connection
    pg.close()
    assert pg._conn is None

    # Next query should auto-reconnect
    result = pg.query("SELECT 2")
    assert pg._conn is not None
    assert not pg._conn.closed
    assert result.iloc[0, 0] == 2


def test_sql_alias():
    """Test that the sql alias works the same as query method."""
    pg = PG()

    # Test that sql is an alias for query
    result1 = pg.query("SELECT 1 as test")
    result2 = pg.sql("SELECT 1 as test")

    assert isinstance(result1, pd.DataFrame)
    assert isinstance(result2, pd.DataFrame)
    assert result1.equals(result2)
