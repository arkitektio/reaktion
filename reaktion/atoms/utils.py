import asyncio
from typing import Awaitable, Callable, Dict
from rekuest.api.schema import AssignationFragment, AssignationLogLevel, NodeKind
from rekuest.messages import Assignation
from rekuest.postmans.utils import ReservationContract
from fluss.api.schema import (
    ArkitektNodeFragment,
    FlowNodeFragment,
    ReactiveImplementationModelInput,
    ReactiveNodeFragment,
)
from reaktion.atoms.arkitekt import ArkitektMapAtom, ArkitektMergeMapAtom
from reaktion.atoms.transformation.chunk import ChunkAtom
from reaktion.atoms.combination.zip import ZipAtom
from reaktion.atoms.combination.withlatest import WithLatestAtom
from reaktion.atoms.combination.combinelatest import CombineLatestAtom
from rekuest.postmans.utils import RPCContract
from .base import Atom
from .transport import AtomTransport


def atomify(
    node: FlowNodeFragment,
    transport: AtomTransport,
    contracts: Dict[str, RPCContract],
    assignation: Assignation,
    alog: Callable[[Assignation, AssignationLogLevel, str], Awaitable[None]] = None,
) -> Atom:

    if isinstance(node, ArkitektNodeFragment):
        if node.kind == NodeKind.FUNCTION:
            return ArkitektMapAtom(
                node=node,
                contract=contracts[node.id],
                transport=transport,
                assignation=assignation,
                alog=alog,
            )
        if node.kind == NodeKind.GENERATOR:
            return ArkitektMergeMapAtom(
                node=node,
                contract=contracts[node.id],
                transport=transport,
                assignation=assignation,
                alog=alog,
            )

    if isinstance(node, ReactiveNodeFragment):
        if node.implementation == ReactiveImplementationModelInput.ZIP:
            return ZipAtom(
                node=node,
                transport=transport,
                assignation=assignation,
                alog=alog,
            )
        if node.implementation == ReactiveImplementationModelInput.CHUNK:
            return ChunkAtom(
                node=node,
                transport=transport,
                assignation=assignation,
                alog=alog,
            )
        if node.implementation == ReactiveImplementationModelInput.WITHLATEST:
            return WithLatestAtom(
                node=node,
                transport=transport,
                assignation=assignation,
                alog=alog,
            )
        if node.implementation == ReactiveImplementationModelInput.COMBINELATEST:
            return WithLatestAtom(
                node=node,
                transport=transport,
                assignation=assignation,
                alog=alog,
            )

    raise NotImplementedError(f"Atom for {node} is not implemented")
