from io import BufferedIOBase, UnsupportedOperation
import pytest
import requests

from openpolicedata.httpio import HTTPIOFile
from openpolicedata import httpio

test_url = 'https://cdn.muckrock.com/foia_files/2024/05/05/Records_Request_Download_FOIL-2023-3998_2024-05-05--23-47-12.zip'

# from __future__ import print_function
# from __future__ import absolute_import

# import unittest
# from unittest import TestCase

# from httpio import HTTPIOFile
# from io import BufferedIOBase, UnsupportedOperation
from io import SEEK_CUR, SEEK_END

# import mock
# import random
# import re

# from six import int2byte

@pytest.fixture(scope="module")
def DATA():
    r = requests.get(test_url)
    r.raise_for_status()
    return r.content

ASCII_LINES = ["Line0\n",
               "Line the first\n",
               "Line Returns\n",
               "Line goes forth"]

def test_implements_buffered_io_base():
    with HTTPIOFile(test_url, 1024) as io:
        assert isinstance(io, BufferedIOBase)

def test_read_after_close_fails():
    with HTTPIOFile(test_url, 1024) as io:
        pass

    with pytest.raises(httpio.IOBaseError):
        io.read()

def test_closed():
    with HTTPIOFile(test_url, 1024) as io:
        assert hasattr(io, 'closed')
        assert not io.closed
    assert io.closed

def test_detach():
    with HTTPIOFile(test_url, 1024) as io:
        with pytest.raises(UnsupportedOperation):
            io.detach()

def test_fileno():
    with HTTPIOFile(test_url) as io:
        with pytest.raises(httpio.IOBaseError):
            io.fileno()

def test_isatty():
    with HTTPIOFile(test_url, 1024) as io:
        assert not io.isatty()

def test_peek(DATA):
    len = 128
    with HTTPIOFile(test_url, len) as io:
        start = 800
        io.seek(start)
        data = io.peek(len)
        assert data == DATA[start:start + len]
        assert io.tell()==start

def test_read_gets_data(DATA):
    with HTTPIOFile(test_url, 1024) as io:
        data = io.read(1024)
        assert data == DATA[0:1024]

def test_read_gets_data_without_buffering(DATA):
    with HTTPIOFile(test_url) as io:
        assert io.read()==DATA

#     def test_throws_exception_when_get_returns_error():
#         with HTTPIOFile(test_url, 1024) as io:
#             self.error_code = 404
#             with self.assertRaises(HTTPException):
#                 io.read(1024)
#             self.assertEqual(io.tell(), 0)

def test_read1(DATA):
    with HTTPIOFile(test_url, 1024) as io:
        io.seek(1024)
        io.read(1024)
        io.seek(0)

        data = io.read1()
        assert data == DATA[:2048]
        
        io.seek(1536)

        data = io.read1()
        assert data == DATA[1536:]

def test_readable():
    with HTTPIOFile(test_url, 1024) as io:
        assert io.readable()

def test_readinto(DATA):
    b = bytearray(1536)
    with HTTPIOFile(test_url, 1024) as io:
        assert io.readinto(b) == len(b)
        assert bytes(b) == DATA[:1536]

def test_readinto1(DATA):
    b = bytearray(len(DATA))
    with HTTPIOFile(test_url, 1024) as io:
        io.seek(1024)
        io.read(1024)
        io.seek(0)

        assert io.readinto1(b) == 2048

        assert b[:2048] == DATA[:2048]
        io.seek(1536)

        assert io.readinto1(b) == len(DATA) - 1536

        assert b[:len(DATA) - 1536] == DATA[1536:]

# def test_readline():
#     self.data_source = ASCII_DATA
#     with HTTPIOFile(test_url, 1024) as io:
#         self.assertEqual(io.readline().decode('ascii'),
#                             ASCII_LINES[0])

#     def test_readlines():
#         self.data_source = ASCII_DATA
#         with HTTPIOFile(test_url, 1024) as io:
#             self.assertEqual([line.decode('ascii') for line in io.readlines()],
#                              [line for line in ASCII_LINES])

    def test_tell_starts_at_zero():
        with HTTPIOFile(test_url, 1024) as io:
            assert io.tell()==0

def test_seek_and_tell_match(DATA):
    with HTTPIOFile(test_url, 1024) as io:
        assert io.seek(1536) == 1536
        assert io.tell() == 1536

        assert io.seek(10, whence=SEEK_CUR) == 1546
        assert io.tell() == 1546

        assert io.seek(-20, whence=SEEK_CUR) == 1526
        assert io.tell() == 1526

        assert io.seek(-20, whence=SEEK_END) == len(DATA) - 20
        assert io.tell() == len(DATA) - 20

def test_random_access(DATA):
    with HTTPIOFile(test_url, 1024) as io:
        io.seek(1536)
        assert io.read(1024) == DATA[1536:2560]
        io.seek(10, whence=SEEK_CUR)
        assert io.read(1024) == DATA[2570:3594]
        io.seek(-20, whence=SEEK_CUR)
        assert io.read(1024) == DATA[3574:4598]
        io.seek(-1044, whence=SEEK_END)
        assert io.read(1024) == DATA[-1044:-20]

def test_seekable():
    with HTTPIOFile(test_url, 1024) as io:
        assert io.seekable()

def test_truncate():
    with HTTPIOFile(test_url, 1024) as io:
        with pytest.raises(httpio.IOBaseError):
            io.truncate()

# We have no need to write
# def test_write():
#     with HTTPIOFile(test_url, 1024) as io:
#         with pytest.raises(httpio.HTTPIOError):
#             io.write(DATA[:1024])

def test_writable():
    with HTTPIOFile(test_url, 1024) as io:
        assert not io.writable()

def test_writelines():
    with HTTPIOFile(test_url, 1024) as io:
        with pytest.raises(httpio.HTTPIOError):
            io.writelines([line.encode('ascii') for line in ASCII_LINES])