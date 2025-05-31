# PostgreSQL Interface for Python

A simple and intuitive Python interface for PostgreSQL database operations with enhanced schema support.

## Features

- **Simple API**: Comprehensive methods for database interaction with schema support
- **Auto DataFrame Conversion**: Query results automatically converted to pandas DataFrames
- **Multi-statement Support**: Execute multiple SQL statements in a single transaction
- **Schema Management**: Create, list, and manage PostgreSQL schemas
- **Smart Table Operations**: Auto-parsing of schema.table format
- **Data Type Mapping**: Intelligent mapping from pandas dtypes to PostgreSQL types
- **Batch Insertion**: Efficient DataFrame insertion with batch processing
- **Auto-reconnect**: Automatic connection management with reconnection capability

## Installation

```bash
# Install dependencies
pip install pandas psycopg python-dotenv

# Or using uv
uv add pandas psycopg python-dotenv
```

## Environment Configuration

Create a `.env` file in your project root:

```env
PG_HOST=localhost
PG_PORT=5432
PG_DBNAME=your_database
PG_USER=your_username
PG_PASSWORD=your_password
```

## Quick Start

```python
from psql.pg import PG
import pandas as pd

# Initialize connection
pg = PG()

# Create a DataFrame
df = pd.DataFrame({
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35],
    'salary': [50000.5, 60000.0, 55000.75]
})

# Insert DataFrame to a specific schema
pg.insert_pg(df, 'hr.employees', overwrite=True)

# Query data
result = pg.query("SELECT * FROM hr.employees WHERE age > 25")
print(result)
```

## API Reference

### Core Methods

#### `query(sql: str) -> pd.DataFrame | None`

Execute SQL queries with support for multiple statements.

```python
# Single query
result = pg.query("SELECT * FROM employees")

# Multiple statements (executed in transaction)
result = pg.query("""
    CREATE TABLE IF NOT EXISTS temp_table (id INT, name TEXT);
    INSERT INTO temp_table VALUES (1, 'Test');
    SELECT * FROM temp_table;
""")
```

#### `insert_pg(df: pd.DataFrame, table_name: str, overwrite: bool = False) -> None`

Insert pandas DataFrame into PostgreSQL with intelligent schema handling.

**Table Name Formats:**
- `'employees'` → Uses default schema `public`
- `'hr.employees'` → Uses schema `hr` with table `employees`

```python
# Insert to default schema (public)
pg.insert_pg(df, 'employees')

# Insert to specific schema (auto-creates schema if needed)
pg.insert_pg(df, 'hr.employees', overwrite=True)

# Append to existing table (truncates first)
pg.insert_pg(df, 'sales.monthly_data', overwrite=False)
```

**Parameters:**
- `df`: pandas DataFrame to insert
- `table_name`: Target table name (supports schema.table format)
- `overwrite`: If True, drops and recreates table; if False, truncates existing table

### Schema Management

#### `create_schema(schema_name: str) -> None`

Create a new schema.

```python
pg.create_schema('analytics')
```

#### `list_schemas() -> pd.DataFrame`

List all available schemas.

```python
schemas = pg.list_schemas()
print(schemas)
```

#### `drop_schema(schema_name: str, cascade: bool = False) -> None`

Drop a schema.

```python
# Drop empty schema
pg.drop_schema('old_schema')

# Drop schema and all contained objects
pg.drop_schema('old_schema', cascade=True)
```

#### `schema_exists(schema_name: str) -> bool`

Check if a schema exists.

```python
if pg.schema_exists('analytics'):
    print("Analytics schema is available")
```

### Table Management

#### `list_tables(schema_name: str = 'public') -> pd.DataFrame`

List all tables in a schema.

```python
# List tables in public schema
tables = pg.list_tables()

# List tables in specific schema
hr_tables = pg.list_tables('hr')
print(hr_tables)
```

#### `describe_table(table_name: str, schema_name: Optional[str] = None) -> pd.DataFrame`

Get detailed table information.

```python
# Using schema.table format
table_info = pg.describe_table('hr.employees')

# Using separate parameters
table_info = pg.describe_table('employees', 'hr')

print(table_info[['column_name', 'data_type', 'is_nullable']])
```

#### `table_exists(table_name: str, schema_name: Optional[str] = None) -> bool`

Check if a table exists.

```python
if pg.table_exists('hr.employees'):
    print("HR employees table exists")
```

### Connection Management

#### `close()`

Manually close the database connection.

```python
pg.close()
```

## Data Type Mapping

The interface automatically maps pandas data types to appropriate PostgreSQL types:

| Pandas Type | PostgreSQL Type |
|-------------|-----------------|
| `int64` (small) | `INTEGER` |
| `int64` (large) | `BIGINT` |
| `float64` | `DOUBLE PRECISION` |
| `bool` | `BOOLEAN` |
| `datetime64` | `TIMESTAMP` |
| `datetime64tz` | `TIMESTAMP WITH TIME ZONE` |
| `string` (short) | `VARCHAR(255)` |
| `string` (long) | `TEXT` |
| `categorical` | `TEXT` |

## Advanced Usage

### Working with Multiple Schemas

```python
# Create departmental schemas
for dept in ['hr', 'finance', 'engineering']:
    pg.create_schema(dept)

# Insert department-specific data
hr_data = pd.DataFrame({...})
finance_data = pd.DataFrame({...})

pg.insert_pg(hr_data, 'hr.employees')
pg.insert_pg(finance_data, 'finance.transactions')

# Query across schemas
result = pg.query("""
    SELECT h.name, f.salary 
    FROM hr.employees h
    JOIN finance.salaries f ON h.id = f.employee_id
""")
```

### Batch Processing

```python
# Large DataFrame insertion (automatically batched)
large_df = pd.DataFrame({
    'id': range(10000),
    'value': np.random.randn(10000)
})

# Efficiently inserted in batches of 1000 rows
pg.insert_pg(large_df, 'analytics.large_dataset', overwrite=True)
```

### Transaction Management

```python
# Multiple operations in single transaction
pg.query("""
    BEGIN;
    UPDATE hr.employees SET salary = salary * 1.05 WHERE department = 'Engineering';
    INSERT INTO hr.salary_history (employee_id, old_salary, new_salary, date) 
    SELECT id, salary/1.05, salary, CURRENT_DATE FROM hr.employees WHERE department = 'Engineering';
    COMMIT;
""")
```

## Error Handling

```python
try:
    pg.insert_pg(df, 'invalid..table.name')
except ValueError as e:
    print(f"Invalid table name format: {e}")

try:
    pg.query("INVALID SQL")
except Exception as e:
    print(f"SQL execution error: {e}")
```

## Best Practices

1. **Use Schema Organization**: Group related tables by department or function
   ```python
   # Good organization
   pg.insert_pg(sales_data, 'sales.monthly_revenue')
   pg.insert_pg(customer_data, 'sales.customers')
   pg.insert_pg(employee_data, 'hr.employees')
   ```

2. **Handle Large Datasets**: The interface automatically batches large inserts
   ```python
   # Automatically optimized for large datasets
   pg.insert_pg(million_row_df, 'analytics.big_data', overwrite=True)
   ```

3. **Use Transactions for Related Operations**:
   ```python
   # Multiple related operations in one transaction
   pg.query("""
       CREATE SCHEMA IF NOT EXISTS reporting;
       CREATE TABLE reporting.monthly_summary AS 
       SELECT department, AVG(salary) as avg_salary 
       FROM hr.employees 
       GROUP BY department;
   """)
   ```

4. **Verify Schema/Table Existence**:
   ```python
   if not pg.schema_exists('analytics'):
       pg.create_schema('analytics')
   
   if not pg.table_exists('analytics.daily_metrics'):
       pg.insert_pg(metrics_df, 'analytics.daily_metrics', overwrite=True)
   ```

## Testing

Run the test suite:

```bash
# Run all tests
pytest tests/

# Run specific test file
pytest tests/test_schema_functionality.py

# Run manual tests
python tests/test_schema_functionality.py
```

## Configuration

All configuration is handled through environment variables:

- `PG_HOST`: PostgreSQL server hostname
- `PG_PORT`: PostgreSQL server port (default: 5432)
- `PG_DBNAME`: Database name
- `PG_USER`: Username for authentication
- `PG_PASSWORD`: Password for authentication

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

This project is licensed under the MIT License.
