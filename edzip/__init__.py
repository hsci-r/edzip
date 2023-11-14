from io import IOBase
import sqlite3
from zipfile import ZipFile, ZipInfo, ZipExtFile, ZIP_STORED
from zipfile import _SharedFile, structFileHeader, sizeFileHeader, BadZipFile, _FH_SIGNATURE, stringFileHeader, _FH_FILENAME_LENGTH, _FH_EXTRA_FIELD_LENGTH # type: ignore
from stream_unzip import stream_unzip
import struct
import os
from typing import Any, Callable, Generator, Optional, Sequence, Union


class _SqliteBackedSequence(Sequence):

    def __init__(self, con: sqlite3.Connection, fields: str, _len: int, conversion: Callable[[tuple], Any]):
        self.con = con
        self.fields = fields
        self.conversion = conversion
        self._len = _len

    def __len__(self):
        return self._len

    def __getitem__(self, index):
        if isinstance(index, slice):
            if index.step is not None:
                raise ValueError("Step not supported")
            if index.start is None:
                return [self.conversion(row) for row in self.con.execute(f"SELECT {self.fields} FROM offsets WHERE file_number < ?", (index.stop,)).fetchall()]
            if index.stop is None:
                return [self.conversion(row) for row in self.con.execute(f"SELECT {self.fields} FROM offsets WHERE file_number >= ?", (index.start,)).fetchall()]
            return [self.conversion(row) for row in
                self.con.execute(f"SELECT {self.fields} FROM offsets WHERE file_number BETWEEN ? AND ?", (index.start, index.stop - 1)).fetchall()]
        else:
            return self.conversion(
                self.con.execute(f"SELECT {self.fields} FROM offsets WHERE file_number == ?", (index,)).fetchone())

    def __iter__(self):
        return map(self.conversion, self.con.execute(f"SELECT {self.fields} FROM offsets"))

    def __reversed__(self):
        return map(self.conversion, self.con.execute(f"SELECT {self.fields} FROM offsets ORDER BY file_number DESC"))


class EDZipFile(ZipFile):
    """A subclass of ZipFile that reads the directory information from an external SQLite database.
    """

    def __init__(self, file: Union[str, os.PathLike, IOBase], con: sqlite3.Connection):
        """Initializes a new instance of the class.

        Args:
            file (str or os.PathLike or BinaryIO): The ZIP file to read from.
            con (sqlite3.Connection): The SQLite3 database connection to the external directory.
        """
        super().__init__(file, 'r', ZIP_STORED, True, None)
        self._len = con.execute("SELECT COUNT(*) FROM offsets").fetchone()[0]
        self.con = con

    def __len__(self) -> int:
        """Return the number of items in the EDZip object.

        Returns:
            int: The number of items in the EDZip object.
        """
        return self._len

    def _RealGetContents(self):
        pass

    def namelist(self) -> Sequence[str]:
        """Returns a sequence of filenames stored in the ZIP archive.

        Returns:
            Sequence[str]: A sequence of filenames.
        """
        return _SqliteBackedSequence(self.con, "filename", self._len, lambda x: x[0])

    def _tuple_to_zinfo(self, tuple) -> ZipInfo:
        zi = ZipInfo(tuple[2])
        zi.compress_size = tuple[1]
        zi.header_offset = tuple[0]
        return zi

    def infolist(self) -> Sequence[ZipInfo]:
        """Return a sequence of ZipInfo objects for all files in the archive.

        Returns:
            Sequence[ZipInfo]: sequence of ZipInfo objects.
                Note that the ZipInfo objects returned have only offset info filled in.
                To get all info, call fillinfo() with each object.
        """
        return _SqliteBackedSequence(self.con, "header_offset,compressed_size,filename", self._len, self._tuple_to_zinfo)

    def getinfo(self, name) -> ZipInfo:
        """Retrieves information about a file in the archive.

        Args:
            name (str): The name of the file to retrieve information for.

        Returns:
            ZipInfo: An object containing offset information for the specified file.
                Note that the object returned has only offset info filled in.
                To get all info, call fillinfo() with it.
        """
        zi = ZipInfo(name)
        (zi.header_offset,zi.compress_size) = self.con.execute("SELECT header_offset,compressed_size FROM offsets WHERE filename = ?",
                                               (name,)).fetchone()
        return zi
    
    def getpositions(self, names: Sequence[str]) -> Generator[int, None, None]:
        """Retrieves the positions of the given files in the archive.

        Args:
            names (Sequence[str]): The names of the files to retrieve positions for.

        Yields:
            int: The position in the archive for each of the given files.
        """
        for row in self.con.execute("SELECT file_number FROM offsets WHERE filename IN (%s)" %
                                                   ','.join('?' * len(names)), names):
            yield row[0]

    def fillinfo(self, zinfo: ZipInfo) -> ZipInfo:
        """Fill the given ZipInfo object with further information about the file in the archive.

        Args:
            zinfo (ZipInfo): The ZipInfo object to fill in with information.

        Returns:
            ZipInfo: The filled-in ZipInfo object.
        """
        self.fp.seek(zinfo.header_offset)
        fheader = self.fp.read(sizeFileHeader)
        if len(fheader) != sizeFileHeader:
            raise BadZipFile("Truncated file header")
        fheader = struct.unpack(structFileHeader, fheader)
        if fheader[_FH_SIGNATURE] != stringFileHeader:
            raise BadZipFile("Bad magic number for file header")
        (zinfo.extract_version, zinfo.reserved,
         zinfo.flag_bits, zinfo.compress_type, t, d,
         zinfo.CRC, zinfo.compress_size, zinfo.file_size) = fheader[1:10]
        zinfo._raw_time = t
        zinfo.date_time = ((d >> 9) + 1980, (d >> 5) & 0xF, d & 0x1F,
                           t >> 11, (t >> 5) & 0x3F, (t & 0x1F) * 2)
        zinfo.extra = self.fp.read(fheader[_FH_EXTRA_FIELD_LENGTH])
        zinfo.orig_filename = self.fp.read(fheader[_FH_FILENAME_LENGTH])
        if fheader[_FH_EXTRA_FIELD_LENGTH]:
            zinfo.extra = self.fp.read(fheader[_FH_EXTRA_FIELD_LENGTH])
            zinfo._decodeExtra()
        return zinfo

    def getinfos(self, names_or_positions: Union[Sequence[str],Sequence[int]]) -> list[ZipInfo]:
        """Returns a generator that yields ZipInfo objects for the given list of filenames or positions in the archive list of files.

        Args:
            names_or_positions (Sequence[str] or Sequence[int]): A list of filenames or positions to retrieve ZipInfo objects for.

        Yields:
            ZipInfo: A ZipInfo object for each filename or position in the input list.
        """
        if isinstance(names_or_positions[0], int):
            return [self._tuple_to_zinfo(tuple) for tuple in self.con.execute("SELECT header_offset,compressed_size,filename FROM offsets WHERE file_number IN (%s)" % ','.join('?' * len(names_or_positions)), names_or_positions).fetchall()]
        else:
            return [self._tuple_to_zinfo(tuple) for tuple in self.con.execute("SELECT header_offset,compressed_size,filename FROM offsets WHERE filename IN (%s)" % ','.join('?' * len(names_or_positions)), names_or_positions).fetchall()]

    def open(self, name: Union[str, ZipInfo], mode: str = "r", pwd: Optional[bytes] = None, *,
             force_zip6: bool = False) -> ZipExtFile:
        """Open the file specified by 'name' inside the ZIP archive for reading.

        Args:
            name (str or ZipInfo): The name of the file to open, or a ZipInfo object.
            mode (str): The mode to open the file in. Only 'r' is supported.
            pwd (bytes): The password to use for decrypting the file, if it is encrypted.
            force_zip64 (bool): Ignored, as this subclass can only be used for reading.

        Returns:
            ZipExtFile: A file-like object for reading the contents of the ZIP archive.
        """
        if mode != "r":
            raise ValueError('This class does not support writing zip files')
        if not self.fp:
            raise ValueError(
                "Attempt to use ZIP archive that was already closed")
        if isinstance(name, str):
            name = self.getinfo(name)
        self.fillinfo(name)
        self._fileRefCnt += 1
        zef_file = _SharedFile(self.fp, self.fp.tell(),
                               self._fpclose, self._lock, lambda: self._writing)
        return ZipExtFile(zef_file, mode, name, pwd, True)

    def stream_from(self, name: Optional[Union[str, ZipInfo]] = None) -> Generator[
        tuple[str, int, Generator[bytes, None, None]], None, None]:
        """Returns a generator that yields a tuple of (filename, file size, file data) for each file in the archive, optionally starting with the specified file.

        Args:
            name (str or ZipInfo): Optional. The name of the file to start streaming from, or a ZipInfo object representing the file.

        Yields:
            (str,int,Generator[bytes,None,None]): tuple of (filename, file size, file data) for each file in the archive.
        """
        if isinstance(name, str):
            # Get info object for name
            name = self.getinfo(name)
        if name is not None:
            self.fp.seek(name.header_offset)
        else:
            self.fp.seek(0)
        return stream_unzip(self.fp)

def create_sqlite_table(con: sqlite3.Connection):
    con.execute("CREATE TABLE offsets (file_number INTEGER PRIMARY KEY, filename TEXT, header_offset INTEGER, compressed_size INTEGER)")

def create_sqlite_indexes(con: sqlite3.Connection):
    con.execute("CREATE UNIQUE INDEX idx_offsets_filename ON offsets (filename)")

def insert_zipinfo_into_sqlite(con: sqlite3.Connection, file_number: int, filename: str, header_offset: int, compressed_size: int):
    con.execute("INSERT INTO offsets (file_number, filename, header_offset, compressed_size) VALUES (?,?,?,?)", (file_number, filename, header_offset, compressed_size))

def create_sqlite_directory_from_zip(zipfile: ZipFile, filename: str) -> sqlite3.Connection:
    """Creates, from the given ZipFile, an SQLite database compatible with ZipFileWithExternalSqliteDirectory.

    Args:
        zipfile (ZipFile): A ZipFile object
        filename (str): The name of the SQLite database file to be created. Will be removed and recreated if it already exists.

    Returns:
        sqlite3.Connection: A connection to the created SQLite database.
    """
    if os.path.exists(filename):
        os.remove(filename)
    con = sqlite3.connect(filename)
    with con:
        create_sqlite_table(con)
    with con:
        for i, zinfo in enumerate(zipfile.infolist()):
            insert_zipinfo_into_sqlite(con, i, zinfo.filename, zinfo.header_offset, zinfo.compress_size)
    with con:
        create_sqlite_indexes(con)
    with con:
        con.execute("VACUUM")
    return con


__all__ = ["EDZipFile", "create_sqlite_table", "create_sqlite_indexes", "insert_zipinfo_into_sqlite", "create_sqlite_directory_from_zip"]