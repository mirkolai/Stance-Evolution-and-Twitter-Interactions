__author__ = 'mirko'
from datetime import datetime
#!/usr/bin/env python
"""
Parameter for logging to mysql
"""
mysql = {
         'host'  : '',
         'user'  : '',
         'passwd': '',
         'db'    : ''}

"""
Parameter for accessing to Twitter's APIs
"""
twitter = {

    'CONSUMER_KEY'    : "",
    'CONSUMER_SECRET' : "",
    'ACCESS_KEY'      : "",
    'ACCESS_SECRET'   : ""

}

"""
Given a date, it returns the corespondent temporal phase
"""
def get_phase(date):
    phase=0

    if   date > datetime(2016, 11, 24, 0, 0, 0, 0)  and \
         date < datetime(2016, 11, 26, 23, 59, 59, 999):
        phase=1
    elif date > datetime(2016, 11, 27, 0, 0, 0, 0) and \
         date < datetime(2016, 11, 29, 23, 59, 59, 999):
        phase=2
    elif date > datetime(2016, 11, 30, 0, 0, 0, 0) and \
         date < datetime(2016, 12, 2, 23, 59, 59, 999):
        phase=3
    elif date > datetime(2016, 12, 4, 0, 0, 0, 0) and \
         date < datetime(2016, 12, 6, 23, 59, 59, 999):
        phase=4
    return phase
