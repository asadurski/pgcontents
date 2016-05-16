"""
Database schema migration utilities.
"""

import glob
import os
import re
from contextlib import contextmanager


def get_local_filepath(filename):
    """
    Helper for finding our raw SQL files locally.

    Expects files to be in:
        alembic/mysql/sqls/
    """
    sqls_dir = os.path.normpath(os.path.join(
        os.path.dirname(__file__),
        'alembic/mysql/sqls'
    ))
    path = glob.glob(os.path.join(sqls_dir, filename))
    if not path:
        raise EnvironmentError("SQL file not found: %s" % filename)
    elif len(path) > 1:
        raise EnvironmentError("SQL file wildcard (%s) is non-unique: %s" %
                               filename, '; '.join(path))
    else:
        return path[0]


@contextmanager
def _multi_statement_execute(conn):
    """Return a context-like function that can execute multiple statements."""
    try:
        raw_conn = conn.connection.connection
    except AttributeError:
        # MockConnection doesn't have connection attribute, but can execute
        # multiple statements just fine.
        yield conn.execute
    else:
        # Other SQL dialects not supported yet.
        assert conn.dialect.driver == 'mysqlconnector'
        # mysqlconnector only allows executing multiple statements with
        # `multi=True` kwarg, but sqlalchemy provides no such API, so we need
        # to reach for the lower levels to do that.
        cursor = raw_conn.cursor(raw=True)
        try:
            yield lambda sql: cursor.execute(sql, multi=True)
        finally:
            cursor.close()


def execute_sql_statements(op, *stmts):
    """Executes the SQL statements using the alembic `op` object
    """
    with _multi_statement_execute(op.get_bind()) as execute:
        lines = '\n'.join("{};".format(s.rstrip(';')) for s in stmts)
        list(execute(lines) or ())


def execute_sql_files(op, *files):
    """Takes the alembic op object as arguments and a list of files as arguments,
    Execute the sql statements in each file.
    """
    with _multi_statement_execute(op.get_bind()) as execute:
        for filename in files:
            sqlfile = get_local_filepath(filename)
            sqls = _parse_sqls(sqlfile)
            # Mysql's cursor.execute returns an interator when multi=True.
            # In dump-sql mode, execute returns None, "or ()" makes the
            # expression return an iterable.
            list(execute(sqls) or ())


_COMMENT_RE = re.compile('--.*')


def _parse_sqls(path):
    """Filter all empty lines and comments lines of the sql file"""
    with open(path, 'r') as fp:
        lines = []
        for line in fp:
            line = _COMMENT_RE.sub('', line)
            if not line.strip():
                continue
            lines.append(line.strip('\n'))
        return '\n'.join(lines)
