
class OPD_GeneralError(Exception):
    def __init__(self, *args):
        super_args = []
        for x in args:
            if hasattr(x, '__iter__') and type(x) != str:
                for y in x:
                    super_args.append(y)
            else:
                super_args.append(x)

        super().__init__(*super_args)

    def append(self, *args):
        new_args = [x for x in self.args]
        for x in args:
            if hasattr(x, '__iter__') and type(x) != str:
                for y in x:
                    new_args.append(y)
            else:
                new_args.append(x)

        self.args = (x for x in new_args)

    def prepend(self, *args):
        new_args = []
        for x in args:
            if hasattr(x, '__iter__') and type(x) != str:
                for y in x:
                    new_args.append(y)
            else:
                new_args.append(x)

        for x in self.args:
            new_args.append(x)

        self.args = (x for x in new_args)

# These exceptions are meant to handle various cases in testing where the URL for datasets are not working
# but there is no issue with OPD
class OPD_DataUnavailableError(OPD_GeneralError):
    pass

class OPD_TooManyRequestsError(OPD_GeneralError):
    pass

class OPD_MultipleErrors(OPD_GeneralError):
    pass

class OPD_arcgisAuthInfoError(OPD_GeneralError):
    pass

class OPD_SocrataHTTPError(OPD_GeneralError):
    pass

class OPD_FutureError(OPD_GeneralError):
    pass

class OPD_MinVersionError(OPD_GeneralError):
    pass

class AutoMergeError(OPD_GeneralError):
    pass

class BadCategoryDict(OPD_GeneralError):
    pass