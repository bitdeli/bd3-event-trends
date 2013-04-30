from datetime import datetime
from itertools import groupby, islice, chain
from bitdeli.model import model, uid
from collections import Counter

NUM_DAYS = 30

@uid
def uid(value):
    return value[value.find(':') + 1:]


def event_names(events):
    for tstamp, group, ip, event in events:
        name = event.get('$event_name', None)
        if name == '$dom_event':
            name = event.get('$event_label', None)
        if name:
            yield name

@model
def build(profiles):
    def parse_day(event):
        return event[0][:10].replace('-', '')
    for profile in profiles:
        uidd = profile.uid
        source_events = chain(profile.get('events', []),
                              profile.get('$dom_event', []))
        days = groupby(source_events, parse_day)
        for day, events in islice(days, NUM_DAYS):
            c = Counter(event_names(events))
            for event, count in c.iteritems():
                yield '%s:%s' % (day, event),\
                      '%d:%s' % (count, uidd) 

