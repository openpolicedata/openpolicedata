Downloaded original version from https://github.com/barneygale/httpio/tree/master on 9/15/2024

httpio
======

HTTP resources as random-access file-like objects

httpio is a small Python library that allows you to access files
served over HTTP as file-like_ objects (which is to say that they
support the interface of the standard library's BufferedIOBase_
class). It differs from libraries like ``urllib`` and ``requests`` in
that it supports ``seek()`` (which moves an internal pointer), and
that ``read()`` makes a request with the ``Range`` header set. It also
supports caching of contents using a configurable block size, and will
reuse TCP connections where possible.

Installation
------------

Use ``pip`` to install httpio:

.. code-block:: console

    $ pip install httpio

Usage
-----

.. code-block:: python

    import zipfile
    import httpio

    url = "http://some/large/file.zip"
    with httpio.open(url) as fp:
        zf = zipfile.ZipFile(fp)
        print(zf.namelist())

.. _file-like: https://docs.python.org/3/glossary.html#term-file-object

.. _BufferedIOBase: https://docs.python.org/3/library/io.html#io.BufferedIOBase

Unit Tests
----------

Unit tests are provided for the standard behaviours implemented by
the library. They can be run with

.. code-block:: console
    
    $ python -m unittest discover -s tests

or a ``tox.ini`` file is provided which allows the tests to be run in
virtual environments using the ``tox`` tool:

.. code-block:: console
    
    $ tox
