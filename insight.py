from collections import Counter
from datetime import datetime, timedelta

from bitdeli.insight import insight, segment, segment_label
from bitdeli.widgets import Line, Text, Widget

NUM_DAYS = 30
MAX_EVENTS = 3

class TokenInput(Widget):
    pass

def unique(events):
    seen = set()
    for event in events:
        if event not in seen:
            yield event
            seen.add(event)

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
    chosen = list(unique(params['events']['value']
                  if 'events' in params else []))[:MAX_EVENTS]

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

@segment
def segment(model, params):
    return ['1', '1000105556']

@segment_label
def label(segment, params):
    return '%d people' % len(segment)
