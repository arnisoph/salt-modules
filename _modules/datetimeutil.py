# -*- coding: utf-8 -*-
"""
The datetime utility module provides functions to gather date and time information
"""

from datetime import datetime


def strftime(format=''):
    """Return a string representing the date and time, controlled by an explicit
    format string. For a complete list of formatting directives, see TODO:Python-Docs

    CLI Example::
      salt myserver datetimeutil.strftime '%H:%M:%S'
    """

    output = datetime.now().strftime(format)

    return output
