import functools
import warnings

def deprecated(msg=None):
    # https://stackoverflow.com/questions/2536307/decorators-in-the-python-standard-lib-deprecated-specifically
    def deprecated_decorator(func):
        """This is a decorator which can be used to mark functions
        as deprecated. It will result in a warning being emitted
        when the function is used."""
        @functools.wraps(func)
        def new_func(*args, **kwargs):
            if msg:
                warning_msg = msg
            else:
                warning_msg = f"{func.__name__} is deprecated and will be removed in a future version."
            warnings.simplefilter('always', DeprecationWarning)  # turn off filter
            warnings.warn(warning_msg,
                        category=DeprecationWarning,
                        stacklevel=2)
            warnings.simplefilter('default', DeprecationWarning)  # reset filter
            return func(*args, **kwargs)
        return new_func
    return deprecated_decorator