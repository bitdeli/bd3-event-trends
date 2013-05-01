from urlparse import urlparse
from datetime import datetime
from itertools import groupby, islice, chain
from bitdeli.model import model, uid
from collections import Counter

NUM_DAYS = 30
MAX_LEN = 64

def event_names(events):
    for tstamp, group, ip, event in events:
        name = event.get('$event_name')
        if name == '$dom_event':
            name = event.get('$event_label')
        elif name == '$pageview':
            name = 'viewed %s' % urlparse(event.get('$page', '')).path
        if name:
            yield name[:MAX_LEN].encode('utf-8')

@model
def build(profiles):
    def parse_day(event):
        return event[0][:10].replace('-', '')
    for profile in profiles:
        uidd = profile.uid
        source_events = chain(profile.get('events', []),
                              profile.get('$dom_event', []),
                              profile.get('$pageview', []))
        days = groupby(source_events, parse_day)
        for day, events in islice(days, NUM_DAYS):
            c = Counter(event_names(events))
            for event, count in c.iteritems():
                yield '%s:%s' % (day, event),\
                      '%d:%s' % (count, uidd) 

@uid
def uid(value):
    return value[value.find(':') + 1:]
