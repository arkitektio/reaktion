from arkitekt.messages.postman.reserve.reserve_transition import ReserveState
from reaktion.atoms.registry import get_atom_registry
from reaktion.atoms import (
    FuncNoParallelArkitektAtom,
    GenNoParallelArkitektAtom,
    WithLatestFromAtom,
)
from arkitekt.schema.enums import NodeType
from typing import Callable, Coroutine, Dict
from herre.wards.base import WardException
from fluss import diagram
from arkitekt.schema import Node
from arkitekt.messages.postman.assign.bounced_assign import Context
import asyncio
import logging

logger = logging.getLogger(__name__)


class EngineError(Exception):
    pass


class NoNodeFoundForIdentifier(EngineError):
    pass


class Engine:
    def __init__(self, di: diagram.Diagram) -> None:
        self.nodes = [node for node in di.elements if isinstance(node, diagram.Node)]
        self.edges = [edge for edge in di.elements if isinstance(edge, diagram.Edge)]

        self.nodeIDNodeMap = {node.id: node for node in self.nodes}
        self.edgeIDEdgeMap = {edge.id: edge for edge in self.edges}

        self.argNode = next(
            (node for node in self.nodes if isinstance(node, diagram.ArgNode)), None
        )  # We only want the first one
        self.kwargNode = next(
            (node for node in self.nodes if isinstance(node, diagram.KwargNode)), None
        )  # We only want the first one
        self.returnNode = next(
            (node for node in self.nodes if isinstance(node, diagram.ReturnNode)), None
        )  # We only want the first one

        self.noneIONodes = [
            node
            for node in di.elements
            if isinstance(node, diagram.Node) and not isinstance(node, diagram.IONode)
        ]
        self.arkitektNodes = [
            node for node in self.nodes if isinstance(node, diagram.ArkitektNode)
        ]
        self.reactiveNodes = [
            node for node in self.nodes if isinstance(node, diagram.ReactiveNode)
        ]

    def connectedEdges(self, node_id: str, sourceHandle: str = None):
        return [
            edge
            for edge in self.edges
            if edge.source == node_id
            and (sourceHandle is None or edge.sourceHandle == sourceHandle)
        ]

    def connectedNodes(
        self, node_id: str, sourceHandle: str = None, targetHandle: str = None
    ):
        edg_ids = [
            edge.target
            for edge in self.connectedEdges(node_id, sourceHandle)
            if targetHandle is None or edge.targetHandle == targetHandle
        ]
        nodes = [node for node in self.nodes if node.id in edg_ids]
        return nodes

    def connectedNodesWithHandle(self, node_id: str, sourceHandle: str = None):
        edges = [edge for edge in self.connectedEdges(node_id, sourceHandle)]
        return [(edge.targetHandle, self.nodeIDNodeMap[edge.target]) for edge in edges]

    def getInitialNodes(self):
        non_kwargs_edge_targets = [
            edge.target for edge in self.edges if edge.source != self.kwargNode.id
        ]
        non_generic_nodes_that_are_not_targets = [
            node
            for node in self.arkitektNodes
            if node.id not in non_kwargs_edge_targets
        ]
        return non_generic_nodes_that_are_not_targets

    def generateConstantsMap(self, kwargs):
        nodeIDConstantsMap: Dict[str, diagram.Constants] = {}

        for kwarg, value in kwargs.items():
            for kwarg_handle, node in self.connectedNodesWithHandle(
                self.kwargNode.id, f"kwarg_{kwarg}"
            ):
                kwarg = kwarg_handle[
                    len("kwarg_") :
                ]  # Trim awai kwarg_ so that key is stillt here
                nodeIDConstantsMap.setdefault(node.id, {})[kwarg] = value

        return nodeIDConstantsMap

    async def generateReservationsMap(
        self, causing_provision: str, context: Context, transition_hook: Coroutine
    ):
        diagramIDs = [node.id for node in self.arkitektNodes]
        nodeSelectors = [node.data.selector for node in self.arkitektNodes]

        nodeInstanceFutures = [
            Node.asyncs.get(id=node.data.node.id) for node in self.arkitektNodes
        ]

        exitstates = [ReserveState.ERROR, ReserveState.CANCELLED, ReserveState.CRITICAL]

        try:
            nodeInstances = await asyncio.gather(*nodeInstanceFutures)
        except WardException as e:
            raise NoNodeFoundForIdentifier(str(e)) from e

        def id_wrapper(id, function):
            async def wrapper(res, state):
                return await function(id, res, state)

            return wrapper

        reservationsContexts = [
            node.reserve(
                **selector.dict(),
                omit_on=[] if selector.crucial else [ReserveState.DISCONNECT],
                exit_on=exitstates + [ReserveState.DISCONNECT]
                if selector.crucial
                else exitstates,
                context=context,
                transition_hook=id_wrapper(id, transition_hook),
                provision=causing_provision,
            )
            for id, node, selector in zip(diagramIDs, nodeInstances, nodeSelectors)
        ]

        reservationEnterFutures = [res.start() for res in reservationsContexts]
        reservations = await asyncio.gather(*reservationEnterFutures)

        return {
            node_id: reservation
            for node_id, reservation in zip(diagramIDs, reservations)
        }

    async def generateAtoms(self, outQueue, constants={}, reservations={}, **kwargs):
        diagramIDAtomMap = {}
        registry = get_atom_registry()
        for diagramNode in self.noneIONodes:
            diagramIDAtomMap[diagramNode.id] = registry.get_atom(diagramNode._type)(
                diagramNode,
                outQueue,
                reservation=reservations.get(diagramNode.id, None),
                constant=constants.get(diagramNode.id, {}),
                **kwargs,
            )

        return diagramIDAtomMap
