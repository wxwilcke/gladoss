#!/usr/bin/env python

def integerRangeArg(arg:str) -> range:
    """ Custom argument type for range

    :param arg: user provided argument string of form 'from:to', ':to', or
        'to', with 'from' and 'to' being positive integers.
    :type arg: str
    :rtype: range
    :returns: range of values to explore
    """

    begin = 0
    arg_lst = arg.split(':')
    try:
        end = int(arg_lst[-1])
        if len(arg_lst) > 1 and len(arg_lst[0]) > 0:
            begin = int(arg_lst[0])
    except:
        raise Exception("'" + arg + "' is not a range of numbers. "
                        + "Expects '0:3', ':3', or '3'.")

    # check if range is valid
    assert begin >= 0 and end >= 0 and begin <= end

    return range(begin, end)
