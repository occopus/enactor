#
# Copyright (C) 2014 MTA SZTAKI
#

"""Enactor module for the OCCO service.

.. moduleauthor:: Adam Visegradi <adam.visegradi@sztaki.mta.hu>

This module is responsible for generating the list of necessary actions to
bring the infrastructure in its desired state.

Input
    - Infrastructure instance identifier

The abstract algorithm is the following:
  #. Query the description of the *desired* state of the infrastructure (static
     description)
  #. Acquire the current state of the infrastructure from the :term:`IB`.
  #. Calculate the *delta* between these two descriptions: the list of lists of
     commands that would bring the infrastructure in its desired state.
  #. Push the delta to the :term:`IP`

.. todo:: Define "list of lists of commands".

"""

__all__ = ['Enactor']

import occo.util as util
import occo.util.factory as factory
import itertools as it
import occo.infobroker as ib
from occo.enactor.downscale import DownscaleStrategy
from occo.enactor.upkeep import Upkeep

class Enactor(object):
    """Maintains a single infrastructure

    :param str infrastructure_id: The identifier of the infrastructure. The
        description will be acquired from the infobroker.

    :param infobroker: The information broker providing information about the
        infrastructure.
    :type infobroker: :class:`occo.infobroker.provider.InfoProvider`

    :param infraprocessor: The InfrastructureProcessor that will handle the
        instructions generated by the Enactor.
    :type infraprocessor:
        :class:`occo.infraprocessor.infraprocessor.AbstractInfraProcessor`
    """
    def __init__(self, infrastructure_id, infraprocessor,
                 downscale_strategy='simple',
                 upkeep_strategy='noop',
                 **config):
        self.infra_id = infrastructure_id
        self.infobroker = ib.main_info_broker
        self.ip = infraprocessor
        self.drop_strategy = DownscaleStrategy.from_config(downscale_strategy)
        self.upkeep = Upkeep.from_config(upkeep_strategy)

    def get_static_description(self, infra_id):
        """Acquires the static description of the infrastructure."""
        # This implementation uses the infobroker to do this
        # Alternatively, the description could be stored in self.
        return self.infobroker.get(
            'infrastructure.static_description', infra_id)
    def calc_target(self, node):
        """
        Calculates the target instance count for the given node
        
        ..todo:: This implementation uses the minimum number of nodes
            specified. A more sophisticated version could use other
            information, and/or scaling functions.
        """

        node.setdefault('scaling', dict(min=1, max=1))
        node['scaling'].setdefault('min', 1)

        return node['scaling']['min']

    def select_nodes_to_drop(self, existing, dropcount):
        """
        Selects ``dropcount`` nodes to be dropped.

        :param int dropcount: The number of nodes to drop.
        :param list existing: Existing node from which to choose.

        .. todo:: This implementation simply selects the last nodes to be
            dropped. This decision should be factored out as a pluggable
            strategy.
        """
        return self.drop_strategy.drop_nodes(existing, dropcount)

    def gen_bootstrap_instructions(self, infra_id):
        """
        Generates a list of instructions to bootstrap the infrastructure.
        
        :param infra_id: Pre-generated infrastructure instance identifier.
            Generated by the :ref:`Compiler <compiler>`
        """ 
        # Currently it seems only the infrastructure needs to be created before
        # the infrastructure is started, and all necessary actions can be
        # encapsulated in this abstract instruction. Nevertheless, this method
        # can be rewritten as necessary.
        if not self.infobroker.get('infrastructure.started', infra_id):
            yield self.ip.cri_create_infrastructure(infra_id=infra_id)

    def calculate_delta(self, static_description, dynamic_state):
        """
        Calculates a list of instructions to be executed to bring the
        infrastructure in its desired state.

        :param static_description: Description of the desired state of the
            infrastructure.
        :type static_description:
            :class:`~occo.compiler.compiler.StaticDescription`

        :param dynamic_state: The actual state of the infrastructure.
        :type dynamic_state: See
            :meth:`occo.infobroker.cloud_provider.CloudInfoProvider.infra_state`

        The result is a list of lists (generator of generators).
        The main result list is called the *delta*. Each item of the delta
        is a list of instructions that can be executed asynchronously and
        independently of each other. Each such a list pertains to a level of
        the topological ordering of the infrastructure.
        
        :rtype:
            .. code::

                delta <generator> = (
                    instructions <generator> = (instr1 <Command>, instr2 <Command>, ...),
                    instructions <generator> = (instr21 <Command>, instr22 <Command>, ...),
                    ...
                )
        """
        # Possibly this is the most complex method in the OCCO, utilizing
        # generators upon generators for efficiency.
        #
        # Actually, it's just five lines constituting only three logical parts.
        # The parts are broken down into smaller parts, all being nested
        # methods inside this one. It may be possible to simplify this method
        # (and it would be desirable if possible), but I couldn't. The biggest
        # problem is with type-matching, as Python cannot do a static type
        # checking, so it is easy to make a mistake. Nevertheless, it currently
        # works and is efficient.
        #
        # Iterators, although efficient, must be used carefully, as they cannot
        # be traversed twice (unlike lists). Typical error: one of them is
        # printed into the log (traversed), and then the actual code will see
        # an empty list (as the generator is finished). But they're extremely
        # memory-efficient, so we have to just deal with it.

        def mk_instructions(fun, nodelist):
            """
            Creates a list of independent instructions based on a single
            topological level. The type of instructions will be determined
            by the logical core function: ``fun``.

            :param list nodelist: List of nodes.
            :param fun: Core function that is called for each node in
                ``nodelist``.
            :type fun: ``(node x [node] x int) -> [command]``.

            ``fun`` returns a *set* (generator) of instructions *for each*
            node. E.g.: when multiple instances of a single node must be
            created at once. If nothing is to be done with a node, an empty
            list is returned by ``fun``.
            
            These individual sets are then unioned, as they all pertain to a
            single topological level (hence ``flatten``)
            """
            return util.flatten( # Union
                fun(node,
                    existing=dynamic_state.get(node['name'], list()),
                    target=self.calc_target(node))
                for node in nodelist)

        def mkdelinst(node, existing, target):
            """
            MaKe DELete INSTructions

            Used as a core to ``mk_instructions``; it creates a list of
            DropNode instructions, for a single node type, as necessary.

            :param node: The node to be acted upon.
            :param existing: Nodes that already exists.
            :param int target: The target number of nodes.
            """
            exst_count = len(existing)
            if target < exst_count:
                return (self.ip.cri_drop_node(instance_data=instance_data)
                        for instance_data in self.select_nodes_to_drop(
                                existing, exst_count - target))
            return []

        def mkcrinst(node, existing, target):
            """
            MaKe CReate INSTructions

            Used as a core to ``mk_instructions``; it creates a list of
            CreateNode instructions, for a single node type, as necessary.

            :param node: The node to be acted upon.
            :param existing: Nodes that already exists.
            :param int target: The target number of nodes.
            """
            exst_count = len(existing)
            if target > exst_count:
                return (self.ip.cri_create_node(node)
                        for i in xrange(target - exst_count))
            return []

        # Shorthand
        infra_id = static_description.infra_id

        # Each `yield' returns an element of the delta

        # The bootstrap elements of the delta, iff needed.
        # This is a single list.
        yield self.gen_bootstrap_instructions(infra_id)

        # Node deletions.
        # Drop instructions are generated for each node, and then they are
        # merged in a single list (as they have no dependencies among them).
        yield util.flatten(mk_instructions(mkdelinst, nodelist)
                           for nodelist in static_description.topological_order)

        # Node creations.
        # Create instructions are generated for each node.
        # Each of these lists pertains to a topological level of the dependency
        # graph, so each of these lists is returned individually.
        for nodelist in static_description.topological_order:
            yield mk_instructions(mkcrinst, nodelist)

    def enact_delta(self, delta):
        """
        Pushes instructions to the :ref:`Infrastructure Processor
        <infraprocessor>`.
        """
        # Push each topological level individually
        for instruction_set in delta:
            # AbstractInfraProcessor.push_instructions accepts list, not
            # generator:
            instruction_list = list(instruction_set)
            # Don't send empty list needlessly
            if instruction_list:
                self.ip.push_instructions(instruction_list)

    def make_a_pass(self):
        """
        Make a maintenance pass on the infrastructure.
        
        .. todo:: We need to implement an "upkeep" phase right before gathering
            information. This means removing dead nodes and other artifacts
            from the system, etc.
        """
        static_description = self.get_static_description(self.infra_id)
        dynamic_state = self.upkeep.acquire_dynamic_state(self.infra_id)
        delta = self.calculate_delta(static_description, dynamic_state)
        self.enact_delta(delta)
