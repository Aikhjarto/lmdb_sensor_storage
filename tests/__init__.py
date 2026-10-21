import os
import shutil
import tempfile
import time

# Run tests in a fixed timezone with daylight saving time, so results don't depend on the timezone of the machine.
# Must be set before datetime objects are converted to local time. Subprocesses inherit it.
os.environ['TZ'] = 'Europe/Vienna'
time.tzset()

# noinspection PyProtectedMember
from lmdb_sensor_storage.db._manager import manager

# Don't sync to disk on each commit, which is slow and not needed for tests
manager.env_kwargs = {'sync': False}


# noinspection PyPep8Naming
class EmptyDatabaseMixin:
    # noinspection PyAttributeOutsideInit
    def setUp(self):
        # logger = logging.getLogger('lmdb_sensor_storage.db')
        # logger.setLevel(logging.DEBUG)

        self.tempfolder = tempfile.mkdtemp()
        self.mdb_filename = os.path.join(self.tempfolder, 'unittest.mdb')

    def tearDown(self) -> None:
        manager.close_all()
        shutil.rmtree(self.tempfolder)
