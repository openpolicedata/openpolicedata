Exceptions
==========

.. currentmodule:: openpolicedata.exceptions

These exception classes represent package-specific loading, filtering, and compatibility failures.

Base Exception
--------------

.. autosummary::
   :toctree: generated/

   OPD_GeneralError

Data Access Exceptions
----------------------

.. autosummary::
   :toctree: generated/

   OPD_DataUnavailableError
   OPD_TooManyRequestsError
   OPD_MultipleErrors
   OPD_arcgisAuthInfoError
   OPD_SocrataHTTPError
   OPD_FutureError
   OPD_MinVersionError
   AutoMergeError
   DateFilterException
   MultiAgencySourceError

Compatibility And Validation Exceptions
---------------------------------------

.. autosummary::
   :toctree: generated/

   BadCategoryDict
   CompatSourceTableLoadError

Module Contents
---------------

.. automodule:: openpolicedata.exceptions
   :no-members: