from collections import Counter
from datetime import datetime, timedelta

from bitdeli.insight import insight
from bitdeli.widgets import Line

NUM_DAYS = 30
MAX_EVENTS = 3

def get_latest(model):
    return max(key.split(':', 1)[0] for key in model)

def get_events(model):
    return frozenset(key.split(':', 1)[1] for key in model)

def daily_count(event, day, model):
    return sum(int(value.split(':', 1)[0])
               for value in model.get('%s:%s' % (day, event), []))

def choose_events(model):
    latest = get_latest(model)
    events = get_events(model)
    if len(events) > MAX_EVENTS:
        c = [(daily_count(event, latest, model), event) for event in events]
        events = [event for count, event in sorted(c)][-MAX_EVENTS:]
    return latest, events

def trend(event, latest_day, model):
    for i in range(NUM_DAYS):
        day = (latest_day - timedelta(days=i)).isoformat()
        yield day, daily_count(event, day.split('T')[0].replace('-', ''), model)

@insight
def view(model, params):
    latest, events = choose_events(model)
    latest_day = datetime.strptime(latest, '%Y%m%d')
    data = [{'label': event, 'data': list(trend(event, latest_day, model))}
            for event in events]
    return [Line(size=(12, 6), data=data)]