# PostgreSQL Interface

A simple, intuitive PostgreSQL interface for Python that makes it easy to:
- Execute SQL queries and get results as pandas DataFrames
- Insert pandas DataFrames into PostgreSQL tables
- Execute multiple SQL statements in a single transaction
- Automatically manage connections and transactions

## Features

- **Simple API**: Just two main methods for database interaction
- **Automatic Connection Management**: Connections are created and managed automatically
- **DataFrame Integration**: Query results are returned as pandas DataFrames, and DataFrames can be inserted into tables
- **Multiple SQL Statements**: Execute multiple SQL statements in a single transaction
- **Auto-commit**: Transactions are automatically committed (configurable)
- **Type Mapping**: Automatic mapping between pandas and PostgreSQL data types

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/psql.git
cd psql

# Install dependencies
pip install -e .
```

## Configuration

Create a `.env` file in the project root with your PostgreSQL connection details:

```
PG_HOST=localhost
PG_PORT=5432
PG_DBNAME=your_database
PG_USER=your_username
PG_PASSWORD=your_password
```

## Usage

### Basic Query

```python
from psql.pg import PG

# Create a PG instance
pg = PG()

# Execute a query and get results as a pandas DataFrame
df = pg.query("SELECT * FROM users LIMIT 10")

# Print the results
print(df)
```

### Multiple SQL Statements

```python
from psql.pg import PG

# Create a PG instance
pg = PG()

# Execute multiple SQL statements in a single transaction
result = pg.query("""
    CREATE TABLE IF NOT EXISTS users (id INTEGER, name TEXT);
    INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
    SELECT * FROM users;
""")

# Print the results of the last statement (SELECT)
print(result)
```

### Insert DataFrame into PostgreSQL

```python
import pandas as pd
from psql.pg import PG

# Create a PG instance
pg = PG()

# Create a DataFrame
df = pd.DataFrame({
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35]
})

# Insert the DataFrame into a table
# If the table doesn't exist, it will be created
# If overwrite=True, the table will be dropped and recreated if it exists
pg.insert_pg(df, 'users', overwrite=True)

# Query the inserted data
result = pg.query("SELECT * FROM users")
print(result)
```

## API Reference

### PG Class

#### Constructor

```python
PG(dbname=PG_DBNAME, host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD)
```

- `dbname`: PostgreSQL database name
- `host`: PostgreSQL host
- `port`: PostgreSQL port
- `user`: PostgreSQL username
- `password`: PostgreSQL password

#### Methods

##### query

```python
query(query: str) -> pd.DataFrame | None
```

Execute a SQL query or multiple SQL statements separated by semicolons.

For multiple statements, they will be executed in a single transaction.
Only the result of the last statement will be returned (if it's a SELECT).

- `query`: SQL query or multiple SQL statements separated by semicolons
- Returns: pandas DataFrame for SELECT queries, None for other queries

##### insert_pg

```python
insert_pg(df: pd.DataFrame, table_name: str, overwrite: bool = False) -> None
```

Insert a pandas DataFrame into a PostgreSQL table.

- `df`: pandas DataFrame to insert
- `table_name`: name of the target table
- `overwrite`: if True, drop and recreate the table if it exists

##### close

```python
close() -> None
```

Close the database connection.

## Running Tests

```bash
pytest
```

## License

MIT
