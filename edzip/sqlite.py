import os
import sqlite3
from typing import Optional
from zipfile import ZipFile

import click
from tqdm import tqdm


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