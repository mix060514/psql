import pytest
import pandas as pd
import numpy as np

from psql.pg import PG

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
    assert list(result.columns) == ['id', 'name']
    assert result['id'].tolist() == [1, 2]
    assert result['name'].tolist() == ['Alice', 'Bob']
    
    # Clean up
    pg.query("DROP TABLE IF EXISTS test_multi;")

def test_insert_pg():
    """Test inserting a pandas DataFrame into a PostgreSQL table."""
    pg = PG()
    
    # Create a test DataFrame
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35],
        'active': [True, False, True],
        'score': [92.5, 88.0, 95.5],
        'created_at': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03'])
    })
    
    # Test creating a new table
    pg.insert_pg(df, 'test_insert', overwrite=True)
    
    # Query the inserted data
    result = pg.query("SELECT * FROM test_insert ORDER BY id")
    
    # Check the results
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 3
    assert list(result.columns) == ['id', 'name', 'age', 'active', 'score', 'created_at']
    assert result['id'].tolist() == [1, 2, 3]
    assert result['name'].tolist() == ['Alice', 'Bob', 'Charlie']
    
    # Test overwriting existing table
    df2 = pd.DataFrame({
        'id': [4, 5],
        'name': ['Dave', 'Eve'],
        'age': [40, 45],
        'active': [False, True],
        'score': [78.5, 82.0],
        'created_at': pd.to_datetime(['2023-01-04', '2023-01-05'])
    })
    
    pg.insert_pg(df2, 'test_insert', overwrite=True)
    
    # Query the new data
    result = pg.query("SELECT * FROM test_insert ORDER BY id")
    
    # Check the results
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert result['id'].tolist() == [4, 5]
    assert result['name'].tolist() == ['Dave', 'Eve']
    
    # Clean up
    pg.query("DROP TABLE IF EXISTS test_insert;")
