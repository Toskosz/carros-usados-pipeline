from contextlib import contextmanager
from sqlalchemy import create_engine


class WarehouseEngine(object):
    def __init__(
        self, db: str, user: str, password: str, host: str, port: int
    ):
        self.conn_url = f'postgresql://{user}:{password}@{host}:{port}/{db}'

    @contextmanager
    def insert_dataframe(self, df):
        engine = create_engine(self.conn_url)
        df.to_sql('table_name', engine)