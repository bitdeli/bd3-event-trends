from collections import Counter
from datetime import datetime, timedelta

from bitdeli.insight import insight, segment, segment_label
from bitdeli.widgets import Line, Text, Widget

NUM_DAYS = 30
MAX_EVENTS = 3
SEGMENT_RANGE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
SEGMENT_LABEL_FORMAT = "%b %d, %Y"

class TokenInput(Widget):
    pass

def unique(events):
    seen = set()
    for event in events:
        if event not in seen:
            yield event
            seen.add(event)

def get_chosen(params, max_events=MAX_EVENTS):
    return list(unique(params['events']['value']
                if 'events' in params else []))[:max_events]
            
def get_latest(model):
    return max(key.split(':', 1)[0] for key in model)

def get_events(model):
    return frozenset(key.split(':', 1)[1] for key in model)

def daily_count(event, day, model):
    return sum(int(value.split(':', 1)[0])
               for value in model.get('%s:%s' % (day, event), []))

def trend(event, latest_day, model):
    for i in range(NUM_DAYS):
        day = (latest_day - timedelta(days=i)).isoformat()
        yield day, daily_count(event, day.split('T')[0].replace('-', ''), model)

@insight
def view(model, params):
    chosen = get_chosen(params)

    yield Text(size=(12, 'auto'),
               label='Analyzing event trends',
               data={'text': "## How many times has X been triggered in a day?\n"})
            
    yield TokenInput(id='events',
                     size=(12, 1),
                     label='Events to display',
                     value=chosen,
                     data=list(get_events(model)))
    
    if chosen:
        latest_day = datetime.strptime(get_latest(model), '%Y%m%d')
        data = [{'label': event, 'data': list(trend(event, latest_day, model))}
                for event in chosen]
        yield Line(id='trends',
                   size=(12, 6),
                   data=data)


def get_segment_dates(timestamps):
    return (datetime.strptime(tstamp, SEGMENT_RANGE_FORMAT)
            for tstamp in timestamps)

def daily_users(event, day, model):
    return (value.split(':', 1)[1]
            for value in model.get('%s:%s' % (day, event), []))

@segment
def segment(model, params):
    chosen = get_chosen(params['params'])
    start, end = get_segment_dates(params['value'])
    users = set()
    for event in chosen:
        for i in range((end - start).days + 1):
            day = (end - timedelta(days=i)).strftime("%Y%m%d")
            users.update(daily_users(event, day, model))                
    return users

def get_label_events(events):
    initial = events[:-1]
    last = events[-1]
    return ", ".join(initial) + " or " + last if initial else last

@segment_label
def label(segment, model, params):
    chosen = get_chosen(params['params'])
    start, end = get_segment_dates(params['value'])
    
    label = 'Users who triggered %s ' % get_label_events(chosen)
    dateformat = SEGMENT_LABEL_FORMAT
    if end - start:
        label = label + 'between %s and %s' % (start.strftime(dateformat),
                                                end.strftime(dateformat))
    else:
        label = label + 'on %s' % start.strftime(dateformat)
    return label
