# -*- coding: utf-8 -*-
"""
A wrapper for disk.usage
"""


def usage():
    """
    A wrapper for disk.usage that returns numbers as integer/ float typed values
    """

    def num(s):
        try:
            return int(s)
        except ValueError:
            return float(s)

    disk_usage = __salt__['disk.usage']()  # NOQA
    ret = {}

    for k, v in disk_usage.iteritems():
        ret[k] = {
            'available': num(v['available']),
            '1K-blocks': num(v['1K-blocks']),
            'used': num(v['used']),
            'capacity': num(v['capacity'].replace('%', '')),
            'filesystem': v['filesystem'],
        }

    return ret
