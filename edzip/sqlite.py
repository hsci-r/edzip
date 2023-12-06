import os
import sqlite3
from typing import Callable, Iterator, Optional, Sequence, TypeVar
from zipfile import ZipFile, ZipInfo

import click
from tqdm import tqdm

from edzip import ExternalDirectory

T_co = TypeVar('T_co', covariant=True)

class _SqliteBackedSequence(Sequence[T_co]):

    def __init__(self, con: sqlite3.Connection, table_name: str, entry_number_field: str, fields: str, _len: int, conversion: Callable[[tuple], T_co]):
        self.con = con
        self.table_name = table_name
        self.entry_number_field = entry_number_field
        self.fields = fields
        self.conversion = conversion
        self._len = _len

    def __len__(self) -> int:
        return self._len

    def __getitem__(self, index) -> T_co | list[T_co]:
        if isinstance(index, slice):
            if index.step is not None:
                raise ValueError("Step not supported")
            if index.start is None:
                return [self.conversion(row) for row in self.con.execute(f"SELECT {self.fields} FROM {self.table_name} WHERE {self.entry_number_field} < ?", (index.stop,)).fetchall()]
            if index.stop is None:
                return [self.conversion(row) for row in self.con.execute(f"SELECT {self.fields} FROM {self.table_name} WHERE {self.entry_number_field} >= ?", (index.start,)).fetchall()]
            return [self.conversion(row) for row in
                self.con.execute(f"SELECT {self.fields} FROM {self.table_name} WHERE {self.entry_number_field} BETWEEN ? AND ?", (index.start, index.stop - 1)).fetchall()]
        else:
            return self.conversion(
                self.con.execute(f"SELECT {self.fields} FROM {self.table_name} WHERE {self.entry_number_field} == ?", (index,)).fetchone())

    def __iter__(self) -> Iterator[T_co]:
        return map(self.conversion, self.con.execute(f"SELECT {self.fields} FROM {self.table_name}"))

    def __reversed__(self) -> Iterator[T_co]:
        return map(self.conversion, self.con.execute(f"SELECT {self.fields} FROM {self.table_name} ORDER BY {self.entry_number_field} DESC"))

class SQLiteExternalDirectory(ExternalDirectory):
    def __init__(self, con: sqlite3.Connection, table_name:str = "offsets", entry_number_field: str = "file_number", filename_field: str = "filename", offset_field: str = "header_offset", compressed_size_field: str = "compressed_size"):
        self.con = con
        self.table_name = table_name
        self.entry_number_field = entry_number_field
        self.filename_field = filename_field
        self.offset_field = offset_field
        self.compressed_size_field = compressed_size_field
        self._len = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

    @property
    def len(self) -> int:
        return self._len
    
    def namelist(self) -> Sequence[str]:
        return _SqliteBackedSequence(self.con, self.table_name, self.entry_number_field, self.filename_field, self._len, lambda x: x[0])
    
    def _tuple_to_zinfo(self, tuple) -> ZipInfo:
        zi = ZipInfo(tuple[2])
        zi.compress_size = tuple[1]
        zi.header_offset = tuple[0]
        return zi

    def infolist(self) -> Sequence[ZipInfo]:
        return _SqliteBackedSequence(self.con, self.table_name, self.entry_number_field, f"{self.offset_field},{self.compressed_size_field},{self.filename_field}", self._len, self._tuple_to_zinfo)
    
    def getinfo(self, name: str) -> ZipInfo:
        zi = ZipInfo(name)
        (zi.header_offset,zi.compress_size) = self.con.execute(f"SELECT {self.offset_field},{self.compressed_size_field} FROM {self.table_name} WHERE {self.filename_field} = ?", (name,)).fetchone()
        return zi

def create_sqlite_table(con: sqlite3.Connection):
    con.execute("CREATE TABLE offsets (file_number INTEGER PRIMARY KEY, filename TEXT, header_offset INTEGER, compressed_size INTEGER)")

def create_sqlite_indexes(con: sqlite3.Connection):
    con.execute("CREATE INDEX idx_offsets_filename ON offsets (filename)")

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
        for i, zinfo in enumerate(tqdm(zipfile.infolist(),unit='entr',dynamic_ncols=True)):
            insert_zipinfo_into_sqlite(con, i, zinfo.filename, zinfo.header_offset, zinfo.compress_size)
    with con:
        create_sqlite_indexes(con)
    with con:
        con.execute("VACUUM")
    return con

@click.command()
@click.argument("filename")
@click.argument("sqlite-filename", required=False)
def main(filename:str, sqlite_filename:Optional[str] = None):
    if sqlite_filename is None:
        sqlite_filename = filename + ".offsets.sqlite3"
    with ZipFile(filename, 'r') as zf:
        create_sqlite_directory_from_zip(zf, sqlite_filename)


if __name__ == "__main__":
    main()