from datetime import datetime
from itertools import groupby
from bitdeli.model import model, uid

def day(hours):
    for hour, count in hours:
        yield datetime.utcfromtimestamp(hour * 3600).strftime('%Y%m%d'), count

@uid
def uid(value):
    return value[value.find(':') + 1:]
        
@model
def build(profiles):
    for profile in profiles:
        uid = profile.uid
        for event, hours in profile['events'].iteritems():
            for date, counts in groupby(day(hours), lambda x: x[0]):
                yield '%s:%s' % (date, event),\
                      '%d:%s' % (sum(count for date, count in counts), uid)
