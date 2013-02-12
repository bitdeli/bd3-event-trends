from bitdeli.insight import insight
from bitdeli.widgets import Text, Bar, Table
from discodb.query import Q, Literal, Clause

@insight
def view(model, params):
    params = {'events': ['$signup', 'Clip created']}
    def steps(events):
        for i in range(len(events)):
            q = Q([Clause([Literal(event)]) for event in events[:i + 1]])
            yield events[i], len(model.query(q))
    return [Bar(size=(12, 6),
                data=list(steps(params['events'])))]
    