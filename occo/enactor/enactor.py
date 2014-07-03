#
# Copyright (C) 2014 MTA SZTAKI
#

__all__ = ['Enactor']

import itertools as it

def skipimap(fun, iterable):
    return it.chain.from_iterable(i for i in it.imap(fun, iterable) if i is not None)

# TODO: think this through
class IPInstruction(dict):
    def __init__(self, **kwargs):
        self.update(kwargs)
class StartNode(IPInstruction):
    def __init__(self, node):
        super(StartNode, self).__init__(instruction='start', node=node)
class StopNode(IPInstruction):
    def __init__(self, node_id):
        super(StartNode, self).__init__(instruction='start', node_id=node_id)
# etc

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
        infra_id = static_description.infra_id

        def mkdelinst(node):
            return None
        def mkcrinst(node):
            existing = len(dynamic_state[node['name']])
            #TODO: do default upon loading
            target = node.get('scaling', dict(min=1, max=1)).get('min', 1)
            if target <= existing:
                return []
            else:
                return (IPInstruction(instruction='start', node=node)
                        for i in xrange(target-existing))

        bootstrap_instructions = [[]]
        if not self.infobroker.get('infrastructure.started',
                                   infra_id=infra_id):
            bootstrap_instructions = [[IPInstruction(instruction='create_enviro',
                                                    infra_id=infra_id)]]
        del_instructions = \
            it.chain.from_iterable(
                skipimap(mkdelinst, nodelist)
                for nodelist in static_description.topological_order)
        cr_instructions = it.chain(skipimap(mkcrinst, nodelist)
                                   for nodelist
                                   in static_description.topological_order)

        return map(list, it.chain(bootstrap_instructions,
                                  del_instructions, cr_instructions))

    def enact_delta(self, delta):
        for iset in delta:
            print '[%s]'%(', '.join('{%s -> %s}'%(i['instruction'],
                                              i.get('node', dict(name=''))['name'])
                                    for i in iset))

    def make_a_pass(self):
        static_description = self.get_static_description(self.infra_id)
        dynamic_state = self.acquire_dynamic_state(self.infra_id)
        delta = self.calculate_delta(static_description, dynamic_state)
        self.enact_delta(delta)
