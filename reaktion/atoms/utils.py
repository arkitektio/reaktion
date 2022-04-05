import asyncio
from typing import Dict
from arkitekt.api.schema import NodeType
from arkitekt.postmans.utils import ReservationContract
from fluss.api.schema import (
    ArkitektNodeFragment,
    FlowNodeFragment,
    ReactiveNodeFragment,
)
from reaktion.atoms.arkitekt import ArkitektMapAtom
from reaktion.atoms.combination.zip import ZipAtom
from .base import Atom


def atomify(
    node: FlowNodeFragment,
    queue: asyncio.Queue,
    contracts: Dict[str, ReservationContract],
) -> Atom:
    if isinstance(node, ArkitektNodeFragment):
        if node.kind == NodeType.FUNCTION:
            return ArkitektMapAtom(
                node=node,
                private_queue=asyncio.Queue(),
                event_queue=queue,
                contract=contracts[node.id],
            )
        if node.kind == NodeType.GENERATOR:
            return ArkitektMapAtom(
                node=node,
                private_queue=asyncio.Queue(),
                event_queue=queue,
                contract=contracts[node.id],
            )

    if isinstance(node, ReactiveNodeFragment):
        if node.implementation == "zip":
            return ZipAtom(
                node=node,
                private_queue=asyncio.Queue(),
                event_queue=queue,
            )

    raise NotImplementedError(f"Atom for {node} is not implemented")
