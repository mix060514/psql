import os 
import psycopg as pg

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
        self.conn = self.connect()

    def connect(self):
        return pg.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password,
        )
    def query(self, query):
        with self.conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall()

# # Connect to an existing database
# # with psycopg.connect("dbname=test user=postgres") as conn:
# with pg.connect("postgresql://postgres:aaaaaaaa@192.168.1.103") as conn:

#     # Open a cursor to perform database operations
#     with conn.cursor() as cur:

#         # Execute a command: this creates a new table
#         cur.execute("""
#             CREATE TABLE test (
#                 id serial PRIMARY KEY,
#                 num integer,
#                 data text)
#             """)

#         # Pass data to fill a query placeholders and let Psycopg perform
#         # the correct conversion (no SQL injections!)
#         cur.execute(
#             "INSERT INTO test (num, data) VALUES (%s, %s)",
#             (100, "abc'def"))

#         # Query the database and obtain data as Python objects.
#         cur.execute("SELECT * FROM test")
#         print(cur.fetchone())
#         # will print (1, 100, "abc'def")

#         # You can use `cur.executemany()` to perform an operation in batch
#         cur.executemany(
#             "INSERT INTO test (num) values (%s)",
#             [(33,), (66,), (99,)])

#         # You can use `cur.fetchmany()`, `cur.fetchall()` to return a list
#         # of several records, or even iterate on the cursor
#         cur.execute("SELECT id, num FROM test order by num")
#         for record in cur:
#             print(record)

#         # Make the changes to the database persistent
#         conn.commit()

def main():
    print("Hello from psql!")
    pg = PG()
    print(pg.query("SELECT * FROM test"))


if __name__ == "__main__":
    main()
