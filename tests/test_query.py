import pytest
import pandas as pd

from psql.pg import PG

def test_query():
    pg = PG()
    df_ = pg.query("SELECT * FROM test limit 2")
    assert isinstance(df_, pd.DataFrame)
