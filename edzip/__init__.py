from abc import ABC, abstractmethod
from io import IOBase
from zipfile import ZipFile, ZipInfo, ZipExtFile, ZIP_STORED
from zipfile import _SharedFile, structFileHeader, sizeFileHeader, BadZipFile, _FH_SIGNATURE, stringFileHeader, _FH_FILENAME_LENGTH, _FH_EXTRA_FIELD_LENGTH # type: ignore
from stream_unzip import stream_unzip
import struct
import os
from typing import Generator, Optional, Sequence, Union

class ExternalDirectory(ABC):
    
    @property
    @abstractmethod
    def len(self) -> int:
        pass

    @abstractmethod
    def namelist(self) -> Sequence[str]:
        pass

    @abstractmethod
    def infolist(self) -> Sequence[ZipInfo]:
        pass

    @abstractmethod
    def getinfo(self, name: str) -> ZipInfo:
        pass

class EDZipFile(ZipFile):
    """A subclass of ZipFile that reads the directory information from an external SQLite database.
    """

    def __init__(self, file: Union[str, os.PathLike, IOBase], ed: ExternalDirectory):
        """Initializes a new instance of the class.

        Args:
            file (str or os.PathLike or BinaryIO): The ZIP file to read from.
            con (sqlite3.Connection): The SQLite3 database connection to the external directory.
        """
        super().__init__(file, 'r', ZIP_STORED, True, None) # type: ignore
        self.ed = ed

    def __len__(self) -> int:
        """Return the number of items in the EDZip object.

        Returns:
            int: The number of items in the EDZip object.
        """
        return self.ed.len

    def _RealGetContents(self):
        pass

    def namelist(self) -> Sequence[str]:
        """Returns a sequence of filenames stored in the ZIP archive.

        Returns:
            Sequence[str]: A sequence of filenames.
        """
        return self.ed.namelist()

    def infolist(self) -> Sequence[ZipInfo]:
        """Return a sequence of ZipInfo objects for all files in the archive.

        Returns:
            Sequence[ZipInfo]: sequence of ZipInfo objects.
                Note that the ZipInfo objects returned have only offset info filled in.
                To get all info, call fillinfo() with each object.
        """
        return self.ed.infolist()

    def getinfo(self, name) -> ZipInfo:
        """Retrieves information about a file in the archive.

        Args:
            name (str): The name of the file to retrieve information for.

        Returns:
            ZipInfo: An object containing offset information for the specified file.
                Note that the object returned has only offset info filled in.
                To get all info, call fillinfo() with it.
        """
        return self.ed.getinfo(name)

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

