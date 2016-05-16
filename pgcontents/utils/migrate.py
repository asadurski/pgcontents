"""
Utilities for running migrations.
"""
from contextlib import contextmanager
from os.path import join
import subprocess

from IPython.utils.tempdir import TemporaryDirectory

from pgcontents.constants import (
    ALEMBIC_INI_TEMPLATE,
    ALEMBIC_DIR_LOCATION,
    ALEMBIC_VERSION_LOCATIONS,
)


@contextmanager
def temp_alembic_ini(alembic_dir_location, sqlalchemy_url, version_locations):
    """
    Temporarily write an alembic.ini file for use with alembic migration
    scripts.
    """
    with TemporaryDirectory() as tempdir:
        alembic_ini_filename = join(tempdir, 'temp_alembic.ini')
        with open(alembic_ini_filename, 'w') as f:
            f.write(
                ALEMBIC_INI_TEMPLATE.format(
                    alembic_dir_location=alembic_dir_location,
                    sqlalchemy_url=sqlalchemy_url,
                    version_locations=version_locations,
                )
            )
        yield alembic_ini_filename


def upgrade(db_url, revision):
    """
    Upgrade the given database to revision.
    """
    with temp_alembic_ini(ALEMBIC_DIR_LOCATION, db_url, ALEMBIC_VERSION_LOCATIONS) as alembic_ini:
        subprocess.check_call(
            ['alembic', '-c', alembic_ini, 'upgrade', revision]
        )
