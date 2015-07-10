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
        dynamic_state = self.infobroker.get('infrastructure.state', infra_id)
        log.debug('%r', dynamic_state)

        nodes = [node
                 for instances in dynamic_state.itervalues()
                 for node in instances.itervalues()]
        failed_nodes, remove_nodes = [], []

        for node in nodes:
            failed = self.is_failed(node)
            shutdown = self.is_shutdown(node)
            if failed or shutdown:
                if failed:
                    failed_nodes.append(node)
                remove_nodes.append(node)
                del dynamic_state[node['name']][node['node_id']]

        log.info('Archiving failed instances of %r: %r',
                 infra_id, [i['node_id'] for i in failed_nodes])
        self.uds.store_failed_nodes(infra_id, *failed_nodes)

        log.info('Removing lost instances from %r: %r',
                 infra_id, [i['node_id'] for i in remove_nodes])
        self.uds.remove_nodes(infra_id, *remove_nodes)

        return dynamic_state
