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

        nodes = (node
                 for instances in stored_state.itervalues()
                 for node in instances.itervalues())
        failed, remove = [], []

        for node in nodes:
            if self.is_failed(node):
                failed.append(node)
                remove.append(node)
            elif self.is_shutdown(node):
                remove.append(node)

        log.info('Archiving failed instances of %r: %r',
                 infra_id, [i['node_id'] for i in failed])
        self.uds.store_failed_nodes(infra_id, *failed)

        log.info('Removing lost instances from %r: %r',
                 infra_id, [i['node_id'] for i in failed])
        self.uds.remove_nodes(infra_id, *remove)

        return updated_state
