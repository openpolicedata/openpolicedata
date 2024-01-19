from enum import Enum
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


def _isinstance(x, type_class):
    if not isinstance(type_class, dict):
        type_class = {'types':[type_class]}
    if 'values' in type_class:
        for v in type_class['values']:
            if x==v:
                return True
    if 'types' in type_class:
        for t in type_class['types']:            
            tf = isinstance(x, t)
            if tf:
                return True
            elif Enum in t.__bases__:
                try:
                    t(x)
                    return True
                except:
                    pass
    return False


def input_swap(idx, names, types, opt1="NO_INPUT", error=False):
    def input_swap_decorator(func):
        @functools.wraps(func)
        def new_func(*args, **kwargs):
            assert len(idx)==2
            assert len(names)==2
            assert len(types)==2
            assert isinstance(types[0], dict) or hasattr(types[0], '__base__')  # Dictionary or a class
            assert isinstance(types[1], dict) or hasattr(types[1], '__base__')  # Dictionary or a class
            assert types[0]!=types[1]

            if idx[1]<idx[0]:
                idx.reverse()
                names.reverse()
                types.reverse()

            # Cases that need handles
            # For original function foo(new_arg1, new_arg0) and new function foo(new_arg0, new_arg1),
            # Passed in reverse order: foo(new_arg1, new_arg0)
            # Passed arg0 in twice: foo(new_arg1, new_arg0=new_arg0)
            # If new_arg0 was originally optional, foo(new_arg1)
            swapped = False
            if     idx[0]<len(args) and _isinstance(args[idx[0]], types[1]):
                if idx[1]<len(args) and _isinstance(args[idx[1]], types[0]):
                    swapped = True
                    args = list(args)
                    args[idx[0]], args[idx[1]] = args[idx[1]], args[idx[0]]
                    args = tuple(args)
                elif idx[1]>=len(args) and names[0] in kwargs:
                    swapped = True
                    kwargs[names[1]] = args[0]
                    args = list(args)
                    args[0] = kwargs.pop(names[0])
                    args = tuple(args)
                elif opt1!="NO_INPUT" and idx[1]>=len(args):
                    swapped = True
                    kwargs[names[1]] = args[0]
                    args = list(args)
                    args[0] = opt1
                    args = tuple(args)

            if swapped:
                error_msg = f"Inputs {idx[0]} ({names[0]}) and {idx[1]} ({names[1]}) of {func.__name__} have been swapped."
                warning_msg = error_msg + " The order has been corrected but this will cause an error in a future version."
                if error:
                    raise ValueError(error_msg)
                else:
                    warnings.simplefilter('always', DeprecationWarning)  # turn off filter
                    warnings.warn(warning_msg,
                                category=DeprecationWarning,
                                stacklevel=2)
                    warnings.simplefilter('default', DeprecationWarning)  # reset filter
            return func(*args, **kwargs)
        return new_func
    return input_swap_decorator