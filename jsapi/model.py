from datetime import datetime
from itertools import groupby, islice, chain
from bitdeli.model import model, uid
from collections import Counter

NUM_DAYS = 30
MAX_URL = 64

# Customize to hide domain from page views
# Example: "bitdeli.com"
URL_DOMAIN = ""

def event_names(events):
    for tstamp, group, ip, event in events:
        name = event.get('$event_name', None)
        if name == '$dom_event':
            name = event.get('$event_label', None)
        elif name == '$pageview':
            if not event.get('$page', ''):
                return
            url = event['$page']
            splitter = URL_DOMAIN if URL_DOMAIN else 'http://'
            if splitter in url:
                url = url.split(splitter, 1)[1]
            url = ('...' + url[-MAX_URL:]) if len(url) > MAX_URL else url
            name = 'Page: %s' % url
        if name:
            yield name

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
