#
# Copyright (C) 2015 MTA SZTAKI
#

"""
Upkeep algorithms to be used before making an Enactor pass.

.. moduleauthor:: Adam Visegradi <adam.visegradi@sztaki.mta.hu>
"""

import occo.infobroker as ib
import occo.util.factory as factory
import logging

log = logging.getLogger('occo.upkeep')

class Upkeep(factory.MultiBackend):
    def __init__(self):
        self.infobroker = ib.main_info_broker

    def acquire_dynamic_state(self, infra_id):
        raise NotImplementedError()

@factory.register(Upkeep, 'noop')
class DefaultUpkeep(Upkeep):
    def acquire_dynamic_state(self, infra_id):
        return self.infobroker.get('infrastructure.state', infra_id)

@factory.register(Upkeep, 'basic')
class BasicUpkeep(Upkeep):
    def __init__(self, uds):
        super(BasicUpkeep, self).__init__()
        self.uds = uds

    def is_failed(self, node):
        """
        .. todo: Update after OCD-187
        """
        return node['state'].startswith('error')

    def is_shutdown(self, node):
        """
        .. todo: Update after OCD-187
        """
        return node['state'].startswith('terminated')

    def acquire_dynamic_state(self, infra_id):
        """
        """
        stored_state = self.infobroker.get('infrastructure.state', infra_id)
        log.debug('%r', stored_state)

        updated_state = dict()
        failed_nodes = updated_state.setdefault(
            '@@failed_nodes', stored_state.get('@@failed_nodes', dict()))

        for node_name, instances in stored_state.iteritems():
            if not node_name.startswith('@@'):
                for node_id, node in instances.iteritems():
                    if self.is_failed(node):
                        failed_nodes[node_id] = node
                    elif self.is_shutdown(node):
                        pass # Drop this node
                    else:
                        insts = updated_state.setdefault(node_name, dict())
                        insts[node_id] = node

        log.debug('Updated state:\n%r', updated_state)

        return updated_state
