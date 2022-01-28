import os
from util.warehouse import DBConnection

def get_warehouse_creds() -> DBConnection:
    return DBConnection(
        user = 'postgres',
        password = 'Thiago123',
        db = 'cars',
        host = 'thiago-note',
        port = 5432
        #user = os.getenv('WAREHOUSE_USER', ''),
        #password = os.getenv('WAREHOUSE_PASSWORD', ''),
        #db = os.getenv('WAREHOUSE_DB', ''),
        #host = os.getenv('WAREHOUSE_HOST', ''),
        #port = int(os.getenv('WAREHOUSE_PORT', 5432)),
    )