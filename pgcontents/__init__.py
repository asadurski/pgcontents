from .checkpoints import PostgresCheckpoints
from .pgmanager import PostgresContentsManager

class MySQLContentsManager(PostgresContentsManager):
    pass

__all__ = [
    'PostgresCheckpoints',
    'PostgresContentsManager',
    'MySQLContentsManager',
]
