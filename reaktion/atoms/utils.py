from typing import Awaitable, Callable, Dict
from rekuest.api.schema import AssignationLogLevel, NodeKind
from rekuest.messages import Assignation
from fluss.api.schema import (
    ArkitektNodeFragment,
    LocalNodeFragment,
    FlowNodeFragment,
    ReactiveImplementationModelInput,
    ReactiveNodeFragment,
    MapStrategy,
)
import asyncio

print(asyncio.Queue)

from reaktion.atoms.arkitekt import (
    ArkitektMapAtom,
    ArkitektMergeMapAtom,
    ArkitektAsCompletedAtom,
    ArkitektOrderedAtom,
)
from reaktion.atoms.local import LocalMapAtom, LocalMergeMapAtom
from reaktion.atoms.transformation.chunk import ChunkAtom
from reaktion.atoms.transformation.split import SplitAtom
from reaktion.atoms.combination.zip import ZipAtom
from reaktion.atoms.combination.withlatest import WithLatestAtom
from rekuest.postmans.utils import RPCContract
from .base import Atom
from .transport import AtomTransport
from rekuest.actors.types import Assignment


def atomify(
    node: FlowNodeFragment,
    transport: AtomTransport,
    contracts: Dict[str, RPCContract],
    assignment: Assignment,
    alog: Callable[[Assignation, AssignationLogLevel, str], Awaitable[None]] = None,
) -> Atom:
    if isinstance(node, ArkitektNodeFragment):
        if node.kind == NodeKind.FUNCTION:
            if node.map_strategy == MapStrategy.MAP:
                return ArkitektMapAtom(
                    node=node,
                    contract=contracts[node.id],
                    transport=transport,
                    assignment=assignment,
                    alog=alog,
                )
            if node.map_strategy == MapStrategy.AS_COMPLETED:
                return ArkitektAsCompletedAtom(
                    node=node,
                    contract=contracts[node.id],
                    transport=transport,
                    assignment=assignment,
                    alog=alog,
                )
            if node.map_strategy == MapStrategy.ORDERED:
                return ArkitektAsCompletedAtom(
                    node=node,
                    contract=contracts[node.id],
                    transport=transport,
                    assignment=assignment,
                    alog=alog,
                )
        if node.kind == NodeKind.GENERATOR:
            return ArkitektMergeMapAtom(
                node=node,
                contract=contracts[node.id],
                transport=transport,
                assignment=assignment,
                alog=alog,
            )

    if isinstance(node, LocalNodeFragment):
        if node.kind == NodeKind.FUNCTION:
            return LocalMapAtom(
                node=node,
                contract=contracts[node.id],
                transport=transport,
                assignment=assignment,
                alog=alog,
            )
        if node.kind == NodeKind.GENERATOR:
            return LocalMergeMapAtom(
                node=node,
                contract=contracts[node.id],
                transport=transport,
                assignment=assignment,
                alog=alog,
            )

    if isinstance(node, ReactiveNodeFragment):
        if node.implementation == ReactiveImplementationModelInput.ZIP:
            return ZipAtom(
                node=node,
                transport=transport,
                assignment=assignment,
                alog=alog,
            )
        if node.implementation == ReactiveImplementationModelInput.CHUNK:
            return ChunkAtom(
                node=node,
                transport=transport,
                assignment=assignment,
                alog=alog,
            )
        if node.implementation == ReactiveImplementationModelInput.WITHLATEST:
            return WithLatestAtom(
                node=node,
                transport=transport,
                assignment=assignment,
                alog=alog,
            )
        if node.implementation == ReactiveImplementationModelInput.COMBINELATEST:
            return WithLatestAtom(
                node=node,
                transport=transport,
                assignment=assignment,
                alog=alog,
            )
        if node.implementation == ReactiveImplementationModelInput.SPLIT:
            return SplitAtom(
                node=node,
                transport=transport,
                assignment=assignment,
                alog=alog,
            )

    raise NotImplementedError(f"Atom for {node} is not implemented")
