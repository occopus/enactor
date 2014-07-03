#
# Copyright (C) 2014 MTA SZTAKI
#

__all__ = ['Enactor']

class Enactor(object):
    def __init__(self, infrastructure_id, infobroker, infraprocessor, **config):
        self.infra_id = infrastructure_id
        self.infobroker = infobroker
        self.infraprocessor = infraprocessor
    def get_static_description(self, infra_id):
        return self.infobroker.get('infrastructure.static_description',
                                   infra_id=infra_id)
    def acquire_dynamic_state(self, infra_id):
        return self.infobroker.get('infrastructure.state', infra_id=infra_id)
    def calculate_delta(self, static_description, dynamic_state):
        pass
    def enact_delta(self, delta):
        pass
    def make_a_pass(self):
        static_description = self.get_static_description(self.infra_id)
        dynamic_state = self.acquire_dynamic_state(self.infra_id)
        delta = self.calculate_delta(static_description, dynamic_state)
        self.enact_delta(delta)
