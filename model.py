from bitdeli.model import model

@model
def build(profiles):
    for profile in profiles:
        for event in profile['events']:
            yield event, profile.uid