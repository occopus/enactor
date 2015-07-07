#
# Copyright (C) 2014 MTA SZTAKI
#

import occo.util.factory as factory

class DownscaleStrategy(factory.MultiBackend):
    """
    Abstract strategy for dropping nodes
    """

    def __init__(self, existing, dropcount):
        pass

    def drop_nodes(self, existing, dropcount):
        raise NotImplementedError()

@factory.register(DownscaleStrategy, 'simple')
class SimpleDownscaleStrategy(DownscaleStrategy):
    """Implements :class:`NodeDropStrategy`, dropping the latest nodes."""
    def drop_nodes(self):
        return existing[-dropcount:]

