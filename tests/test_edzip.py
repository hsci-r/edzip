from io import BytesIO
import unittest
from zipfile import ZipFile, ZipInfo

from edzip import EDZipFile
from edzip.sqlite import create_sqlite_directory_from_zip

class TestCreateSqliteDirectoryFromZip(unittest.TestCase):

    def setUp(self):
        buffer = BytesIO()
        with ZipFile(buffer, "w") as zf:
            zf.writestr("test.txt", "Hello, world!")
            zf.writestr("test2.txt", "Hello again!")
            zf.writestr("test3.txt", "Goodbye!")
            con = create_sqlite_directory_from_zip(zf, ":memory:")
        self.zip_file = ZipFile(buffer, 'r')
        self.edzip_file = EDZipFile(buffer, con)

    def tearDown(self):
        self.edzip_file.close()

    def test_namelist(self):
        self.assertEqual(list(self.edzip_file.namelist()), ["test.txt", "test2.txt", "test3.txt"])
        self.assertEqual(list(self.edzip_file.namelist().__reversed__()), ["test3.txt", "test2.txt", "test.txt"])
    
    def test_fillinfo(self):
        infolist = list(map(lambda x: x.FileHeader(), map(self.edzip_file.fillinfo,self.edzip_file.infolist())))
        self.assertEqual(infolist, list(map(lambda x: x.FileHeader(), self.zip_file.infolist())))

    def test_namelist_slicing(self):
        namelist = self.edzip_file.namelist()
        slice = namelist[1:3]
        self.assertEqual(len(slice), 2)
        self.assertEqual(slice[0], 'test2.txt')
        self.assertEqual(slice[1], 'test3.txt')   
        slice = namelist[:2]
        self.assertEqual(len(slice), 2)
        self.assertEqual(slice[0], 'test.txt')
        self.assertEqual(slice[1], 'test2.txt')
        slice = namelist[1:]
        self.assertEqual(len(slice), 2)
        self.assertEqual(slice[0], 'test2.txt')
        self.assertEqual(slice[1], 'test3.txt')

    def test_positions(self):
        self.assertEqual(list(self.edzip_file.getpositions(['test.txt','test3.txt'])), [0, 2])

    def test_infolist(self):
        infolist = self.edzip_file.infolist()
        self.assertEqual(len(infolist), 3)
        self.assertIsInstance(infolist[0], ZipInfo)
        self.assertEqual(infolist[0].filename, "test.txt")
        self.assertEqual(infolist[0].header_offset, 0)
        self.assertEqual(infolist[1].filename, "test2.txt")
        self.assertGreater(infolist[1].header_offset, infolist[0].header_offset)
        self.assertEqual(infolist[2].filename, "test3.txt")
        self.assertGreater(infolist[2].header_offset, infolist[1].header_offset)

    def test_getinfo(self):
        info = self.edzip_file.getinfo("test2.txt")
        self.assertIsInstance(info, ZipInfo)
        self.assertEqual(info.filename, "test2.txt")
        self.assertGreater(info.header_offset, 0)

    def test_getinfos_by_name(self):
        infos = list(self.edzip_file.getinfos(["test.txt", "test3.txt"]))
        self.assertEqual(len(infos), 2)
        self.assertIsInstance(infos[0], ZipInfo)
        self.assertEqual(infos[0].filename, "test.txt")
        self.assertEqual(infos[0].header_offset, 0)
        self.assertIsInstance(infos[1], ZipInfo)
        self.assertEqual(infos[1].filename, "test3.txt")
        self.assertGreater(infos[1].header_offset,infos[0].header_offset)

    def test_getinfos_by_position(self):
        infos = list(self.edzip_file.getinfos([1, 2]))
        self.assertEqual(len(infos), 2)
        self.assertIsInstance(infos[0], ZipInfo)
        self.assertEqual(infos[0].filename, "test2.txt")
        self.assertGreater(infos[0].header_offset, 0)
        self.assertIsInstance(infos[1], ZipInfo)
        self.assertEqual(infos[1].filename, "test3.txt")
        self.assertGreater(infos[1].header_offset,infos[0].header_offset)

    def test_open(self):
        with self.edzip_file.open("test2.txt") as f:
            self.assertEqual(f.read(), b"Hello again!")

    def test_stream_from(self):
        stream = self.edzip_file.stream_from()
        (filename, size, data) = next(stream)
        self.assertEqual(filename, b"test.txt")
        self.assertEqual(size, 13)
        self.assertEqual(next(data), b"Hello, world!")
        stream = self.edzip_file.stream_from("test2.txt")
        (filename, size, data) = next(stream)
        self.assertEqual(filename, b"test2.txt")
        self.assertEqual(size, 12)
        self.assertEqual(next(data), b"Hello again!")
        (filename, size, data) = next(stream)
        self.assertEqual(filename, b"test3.txt")