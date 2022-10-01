from contextlib import contextmanager
from dataclasses import dataclass
import psycopg2

# @dataclass used for reduced code repetition and cost
# for more information: https://pt.stackoverflow.com/questions/376306/o-que-s%C3%A3o-dataclasses-e-quando-utiliz%C3%A1-las
@dataclass
class DBConnection:
    db: str
    user: str
    password: str
    host: str
    port: int = 5432

class WarehouseConnection:
    def __init__(self, db_conn: DBConnection):
        self.conn_url = (
            f'postgresql://{db_conn.user}:{db_conn.password}@'
            f'{db_conn.host}:{db_conn.port}/{db_conn.db}'
        )

    # Python's resource manager, automatically managing resources
    # for more information: https://www.geeksforgeeks.org/context-manager-using-contextmanager-decorator/
    @contextmanager
    def managed_cursor(self, cursor_factory=None):
        self.conn = psycopg2.connect(self.conn_url)

        # autocommit mode: this way all the commands executed will be immediately committed and no rollback is possible.
        self.conn.autocommit = True
        self.curr = self.conn.cursor(cursor_factory=cursor_factory)
        try:
            # yield is a keyword that is used like return, except the function will return a generator.
            # for more: https://stackoverflow.com/questions/231767/what-does-the-yield-keyword-do
            yield self.curr
        finally:
            self.curr.close()
            self.conn.close()
