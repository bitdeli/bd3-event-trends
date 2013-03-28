from datetime import datetime
from itertools import groupby, islice
from bitdeli.model import model, uid
from collections import Counter

NUM_DAYS = 30

@uid
def uid(value):
    return value[value.find(':') + 1:]
        
@model
def build(profiles):
    def parse_day(event):
        return event[0][:10].replace('-', '')
    for profile in profiles:
        uidd = profile.uid
        days = groupby(profile['events'], parse_day)
        for day, events in islice(days, NUM_DAYS):
            c = Counter(event['$event_name']
                        for tstamp, group, ip, event in events)
            for event, count in c.iteritems():
                yield '%s:%s' % (day, event),\
                      '%d:%s' % (count, uidd) 

