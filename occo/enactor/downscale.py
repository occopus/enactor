#
# Copyright (C) 2014 MTA SZTAKI
#

import occo.util.factory as factory
import random

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
        return existing.values()[-dropcount:]

@factory.register(DownscaleStrategy, 'random')
class RandomDownscaleStrategy(DownscaleStrategy):
    """Implements :class: `DownscaleStrategy`, dropping random nodes."""
    def drop_nodes(self, existing, dropcount):
        return random.sample(existing.values(), dropcount)

