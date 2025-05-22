import pandas as pd
import numpy as np
from psql.pg import PG


def demonstrate_query():
    """Demonstrate the basic query functionality."""
    print("\n=== Basic Query Example ===")
    pg = PG()
    
    # Simple SELECT query
    print("Executing simple SELECT query...")
    df = pg.query("SELECT * FROM test LIMIT 2")
    
    print(f"Result type: {type(df)}")
    if df is not None:
        print(f"Shape: {df.shape}")
        print(f"Columns: {df.columns.tolist()}")
        print(f"Data types:\n{df.dtypes}")
        print("\nData:")
        print(df)
    else:
        print("No data returned from query")


def demonstrate_multiple_statements():
    """Demonstrate executing multiple SQL statements in a single query."""
    print("\n=== Multiple SQL Statements Example ===")
    pg = PG()
    
    print("Executing multiple SQL statements in a single query...")
    result = pg.query("""
        DROP TABLE IF EXISTS demo_multi;
        CREATE TABLE demo_multi (id INTEGER, name TEXT, created_at TIMESTAMP);
        INSERT INTO demo_multi VALUES 
            (1, 'Alice', '2023-01-01'), 
            (2, 'Bob', '2023-01-02'),
            (3, 'Charlie', '2023-01-03');
        SELECT * FROM demo_multi ORDER BY id;
    """)
    
    print("\nResult from the last statement (SELECT):")
    print(result)
    
    # Clean up
    pg.query("DROP TABLE IF EXISTS demo_multi;")


def demonstrate_insert_pg():
    """Demonstrate inserting a pandas DataFrame into a PostgreSQL table."""
    print("\n=== DataFrame Insertion Example ===")
    pg = PG()
    
    # Create a test DataFrame
    print("Creating a sample DataFrame...")
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35],
        'active': [True, False, True],
        'score': [92.5, 88.0, 95.5],
        'created_at': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03'])
    })
    
    print("\nSample DataFrame:")
    print(df)
    
    # Insert DataFrame into a new table
    print("\nInserting DataFrame into a new table 'demo_insert'...")
    pg.insert_pg(df, 'demo_insert', overwrite=True)
    
    # Query the inserted data
    print("\nQuerying the inserted data:")
    result = pg.query("SELECT * FROM demo_insert ORDER BY id")
    print(result)
    
    # Demonstrate overwrite functionality
    print("\nCreating a new DataFrame for overwrite demonstration:")
    df2 = pd.DataFrame({
        'id': [4, 5],
        'name': ['Dave', 'Eve'],
        'age': [40, 45],
        'active': [False, True],
        'score': [78.5, 82.0],
        'created_at': pd.to_datetime(['2023-01-04', '2023-01-05'])
    })
    print(df2)
    
    print("\nOverwriting the existing table with new data...")
    pg.insert_pg(df2, 'demo_insert', overwrite=True)
    
    print("\nQuerying the overwritten data:")
    result = pg.query("SELECT * FROM demo_insert ORDER BY id")
    print(result)
    
    # Clean up
    pg.query("DROP TABLE IF EXISTS demo_insert;")


def main():
    print("=== PostgreSQL Interface Demo ===")
    
    # Demonstrate basic query functionality
    demonstrate_query()
    
    # Demonstrate multiple SQL statements
    demonstrate_multiple_statements()
    
    # Demonstrate DataFrame insertion
    demonstrate_insert_pg()
    
    print("\n=== Demo Complete ===")


if __name__ == "__main__":
    main()
