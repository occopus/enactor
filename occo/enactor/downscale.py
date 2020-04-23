### Copyright 2014, MTA SZTAKI, www.sztaki.hu
###
### Licensed under the Apache License, Version 2.0 (the "License");
### you may not use this file except in compliance with the License.
### You may obtain a copy of the License at
###
###    http://www.apache.org/licenses/LICENSE-2.0
###
### Unless required by applicable law or agreed to in writing, software
### distributed under the License is distributed on an "AS IS" BASIS,
### WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
### See the License for the specific language governing permissions and
### limitations under the License.

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
        nodes = sorted(list(existing.values()), key=lambda k: k['instance_start_time'])[-dropcount:]
        log.debug('Selected nodes (last N) for downscaling: %r', nodes)
        return nodes

@factory.register(DownscaleStrategy, 'random')
class RandomDownscaleStrategy(DownscaleStrategy):
    """Implements :class: `DownscaleStrategy`, dropping random nodes."""
    def drop_nodes(self, existing, dropcount):
        nodes = random.sample(list(existing.values()), dropcount)
        log.debug('Selected nodes (randomly) for downscaling: %r', nodes)
        return nodes

