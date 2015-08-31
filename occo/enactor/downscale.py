#
# Copyright (C) 2014 MTA SZTAKI
#

import occo.util.factory as factory
import random
import logging

log = logging.getLogger('occo.enactor.downscale')

class DownscaleStrategy(factory.MultiBackend):
    """
    Abstract strategy for dropping nodes
    """

    def __init__(self):
        pass

    def drop_nodes(self, existing, dropcount):
        raise NotImplementedError()

@factory.register(DownscaleStrategy, 'simple')
class SimpleDownscaleStrategy(DownscaleStrategy):
    """Implements :class:`DownscaleStrategy`, dropping the latest nodes."""
    def drop_nodes(self, existing, dropcount):
        nodes = existing.values()[-dropcount:]
        log.debug('Selected nodes (last N) for downscaling: %r', nodes)
        return nodes

@factory.register(DownscaleStrategy, 'random')
class RandomDownscaleStrategy(DownscaleStrategy):
    """Implements :class: `DownscaleStrategy`, dropping random nodes."""
    def drop_nodes(self, existing, dropcount):
        nodes = random.sample(existing.values(), dropcount)
        log.debug('Selected nodes (randomly) for downscaling: %r', nodes)
        return nodes

