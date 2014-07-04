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
    def calc_target(self, node):
        return node.get('scaling', dict(min=1, max=1)).get('min', 1)
    def select_nodes_to_drop(self, existing, dropcount):
        # Select last <dropcount> nodes to be dropped
        return existing[-dropcount:]
    def calculate_delta(self, static_description, dynamic_state):
        infra_id = static_description.infra_id

        def mk_instructions(fun, nodelist):
            for node in nodelist:
                inst = fun(node,
                           existing=dynamic_state[node['name']],
                           target=self.calc_target(node))
                if inst is None:
                    continue
                for i in inst:
                    yield i
            return
            yield

        def mkdelinst(node, existing, target):
            exst_count = len(existing)
            if target < exst_count:
                return (IPInstruction(instruction='drop', node_id=node_id)
                        for node_id in self.select_nodes_to_drop(
                                existing, exst_count - target))
        def mkcrinst(node, existing, target):
            exst_count = len(existing)
            if target > exst_count:
                return (IPInstruction(instruction='start', node=node)
                        for i in xrange(target - exst_count))

        bootstrap_instructions = []
        if not self.infobroker.get('infrastructure.started',
                                   infra_id=infra_id):
            bootstrap_instructions.append(
                IPInstruction(instruction='create_enviro', infra_id=infra_id))
        del_instructions = it.chain.from_iterable(
            mk_instructions(mkdelinst, nodelist)
            for nodelist in static_description.topological_order)
        cr_instructions = (mk_instructions(mkcrinst, nodelist)
                           for nodelist in static_description.topological_order)

        return [bootstrap_instructions, del_instructions] + map(list, cr_instructions)

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
