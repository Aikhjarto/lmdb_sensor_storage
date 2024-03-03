import os
import shutil
import tempfile

from lmdb_sensor_storage.db._manager import manager


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