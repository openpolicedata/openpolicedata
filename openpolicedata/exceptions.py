
# These exceptions are meant to handle various cases in testing where the URL for datasets are not working
# but there is no issue with OPD
class OPD_DataUnavailableError(Exception):
    pass

class OPD_TooManyRequestsError(Exception):
    pass

class OPD_MultipleErrors(Exception):
    pass

class OPD_arcgisAuthInfoError(Exception):
    pass