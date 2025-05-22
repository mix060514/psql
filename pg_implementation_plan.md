# PostgreSQL Interface Implementation Plan

## Overview

This document outlines the implementation plan for enhancing the PostgreSQL interface class. The goal is to create a simple, intuitive API for interacting with PostgreSQL databases, with a focus on:

1. Enhanced query execution with support for multiple SQL statements
2. DataFrame insertion capabilities

## Current Implementation

The current implementation in `psql/pg.py` provides:
- Basic connection management with auto-reconnect
- Query execution with automatic conversion to pandas DataFrames
- Auto-commit functionality
- Connection cleanup

## Planned Enhancements

### 1. Enhanced Query Method

The existing `query` method will be enhanced to handle multiple SQL statements separated by semicolons. These statements will be executed within a single transaction, with results from the last statement returned (if it's a SELECT).

```python
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
    statements = [stmt.strip() for stmt in query.split(';') if stmt.strip()]
    
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
            raise Exception(f"Error executing statement {i+1}: {str(e)}")
    
    return result
```

### 2. DataFrame Insertion Method

A new `insert_pg` method will be added to insert pandas DataFrames into PostgreSQL tables. This method will handle table creation, data type mapping, and efficient batch insertion.

```python
def insert_pg(self, df: pd.DataFrame, table_name: str, overwrite: bool = False) -> None:
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
        self.query(f"DROP TABLE IF EXISTS {table_name}; CREATE TABLE {table_name} ({columns});")
    elif not table_exists:
        # Get column types from DataFrame
        pg_types = self._get_pg_types(df)
        
        # Create column definitions
        columns = ", ".join([f"{col} {pg_types[col]}" for col in df.columns])
        
        # Create new table
        self.query(f"CREATE TABLE {table_name} ({columns});")
    elif table_exists and not overwrite:
        # Truncate existing table
        self.query(f"TRUNCATE TABLE {table_name};")
    
    # Insert data in batches
    batch_size = 1000
    total_rows = len(df)
    
    for i in range(0, total_rows, batch_size):
        batch = df.iloc[i:i+batch_size]
        
        # Create value placeholders
        placeholders = []
        values = []
        
        for _, row in batch.iterrows():
            row_values = []
            for val in row:
                if pd.isna(val):
                    row_values.append("NULL")
                elif isinstance(val, str):
                    # Escape single quotes
                    escaped_val = val.replace("'", "''")
                    row_values.append(f"'{escaped_val}'")
                elif isinstance(val, (int, float)):
                    row_values.append(str(val))
                elif isinstance(val, (pd.Timestamp, pd.DatetimeTZDtype)):
                    row_values.append(f"'{val}'")
                else:
                    row_values.append(f"'{val}'")
            
            placeholders.append(f"({', '.join(row_values)})")
        
        # Execute insert
        insert_query = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES {', '.join(placeholders)};"
        self.query(insert_query)
```

### 3. Helper Method for Data Type Mapping

A private helper method will be added to map pandas data types to PostgreSQL data types:

```python
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
        elif pd.api.types.is_datetime64tz_dtype(dtype):
            pg_types[col] = "TIMESTAMP WITH TIME ZONE"
        else:
            pg_types[col] = "TEXT"
    
    return pg_types
```

## Complete Implementation

The complete implementation will include:

1. The enhanced `query` method
2. The new `insert_pg` method
3. The helper `_get_pg_types` method
4. All existing functionality (connection management, auto-commit, etc.)

## Usage Examples

### Multiple SQL Statements

```python
# Execute multiple SQL statements in a transaction
pg = PG()
result = pg.query("""
    CREATE TABLE IF NOT EXISTS test_table (id INTEGER, name TEXT);
    INSERT INTO test_table VALUES (1, 'Alice'), (2, 'Bob');
    SELECT * FROM test_table;
""")
print(result)  # Shows the result of the SELECT query
```

### DataFrame Insertion

```python
# Create a DataFrame
import pandas as pd
df = pd.DataFrame({
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35]
})

# Insert into PostgreSQL
pg = PG()
pg.insert_pg(df, 'users', overwrite=True)

# Query the inserted data
result = pg.query("SELECT * FROM users")
print(result)
```

## Next Steps

To implement these changes:

1. Switch to Code mode
2. Update the `psql/pg.py` file with the enhanced implementation
3. Add tests for the new functionality
