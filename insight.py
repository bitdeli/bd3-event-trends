from collections import Counter, namedtuple
from datetime import datetime, timedelta

from bitdeli.insight import insight, segment, segment_label
from bitdeli.widgets import Line, Text, Widget

NUM_DAYS = 30
MAX_EVENTS = 3
SEGMENT_RANGE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
SEGMENT_LABEL_FORMAT = "%b %d, %Y"

TRENDS_CAPTION = """
### How many times has an event been triggered in a day?

Enter up to %d events below to see their trends over time.
""" % MAX_EVENTS

DIFF_CAPTION = """
### How many times have {0} triggered an event in a day compared to {1}?

Enter an event below to compare its trends in these segments over time.
"""

class TokenInput(Widget):
    pass

def unique(events):
    seen = set()
    for event in events:
        if event not in seen:
            yield event
            seen.add(event)

def get_chosen(params, model):
    max_events = 1 if hasattr(model, 'segments') else MAX_EVENTS
    return list(unique(params['events']['value']
                if 'events' in params else []))[:max_events]
            
def get_latest(model):
    return max(key.split(':', 1)[0] for key in model)

def get_events(model):
    return frozenset(key.split(':', 1)[1] for key in model)

def get_caption(model):
    caption = TRENDS_CAPTION
    caption_label = 'Analyzing event trends'
    if hasattr(model, 'segments'):
        n = len(model.segments)
        suffix = 'another segment'
        segment_labels = model.labels
        if n == 1:
            suffix = 'all users'
            segment_labels.append(suffix)
        caption_label = 'Comparing a segment to ' + suffix
        caption = DIFF_CAPTION.format(*segment_labels)
    return caption, caption_label

def count_by_segment(values, segment=None):
    for value in values:
        if segment is None or value.split(':', 1)[1] in segment:
            yield int(value.split(':', 1)[0])

def daily_count(event, day, model, segment=None):
    return sum(count_by_segment(model.get('%s:%s' % (day, event), []),
                                segment))

def trend(event, latest_day, model, segment=None):
    for i in range(NUM_DAYS):
        day = (latest_day - timedelta(days=i)).isoformat()
        yield day, daily_count(event, day.split('T')[0].replace('-', ''),
                               model, segment)

@insight
def view(model, params):
    def test_segment():
        import random
        random.seed(21)
        labels = ['First Segment']#, 'Second']
        segments = [frozenset(random.sample(model.unique_values(), 100))]
                    #frozenset(random.sample(model.unique_values(), 200))]
        return namedtuple('SegmentInfo', ('model', 'segments', 'labels'))\
                         (model, segments, labels)
        
    #model = test_segment()
    has_segments = hasattr(model, 'segments')
    omodel = model.model if has_segments else model

    latest_day = datetime.strptime(get_latest(omodel), '%Y%m%d')    
    chosen = get_chosen(params, model)
    data = []
    if has_segments and chosen:
        n = len(model.segments)
        event = chosen[0]
        data = [{'label': model.labels[i],
                 'data': list(trend(event, latest_day, omodel, model.segments[i]))}
                for i in range(n)]
        if n == 1:
            data.append({'label': 'All users',
                         'data': list(trend(event, latest_day, omodel))})
    else:
        data = [{'label': event, 'data': list(trend(event, latest_day, omodel))}
                for event in chosen]
    
    caption, caption_label = get_caption(model)
    yield Text(size=(12, 'auto'),
               label=caption_label,
               data={'text': caption})
            
    yield TokenInput(id='events',
                     size=(12, 1),
                     label='Event used for comparison' if has_segments
                           else 'Events to display',
                     value=chosen,
                     data=list(get_events(omodel)))
    
    if data:
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
    chosen = get_chosen(params['params'], model)
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
    chosen = get_chosen(params['params'], model)
    start, end = get_segment_dates(params['value'])
    
    label = 'Users who triggered %s ' % get_label_events(chosen)
    dateformat = SEGMENT_LABEL_FORMAT
    if end - start:
        label = label + 'between %s and %s' % (start.strftime(dateformat),
                                                end.strftime(dateformat))
    else:
        label = label + 'on %s' % start.strftime(dateformat)
    return label
