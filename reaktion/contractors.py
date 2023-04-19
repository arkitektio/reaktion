from typing import Protocol, runtime_checkable
from rekuest.postmans.utils import RPCContract, arkiuse, localuse, mockuse
from fluss.api.schema import (
    ArkitektNodeFragment,
    FlowNodeFragmentBaseArkitektNode,
    LocalNodeFragment,
)
from rekuest.api.schema import afind, ReserveBindsInput
from rekuest.postmans.vars import get_current_postman
from rekuest.structures.registry import get_current_structure_registry
from rekuest.actors.base import Actor


@runtime_checkable
class NodeContractor(Protocol):
    async def __call__(self, node: ArkitektNodeFragment, actor: Actor) -> RPCContract:
        ...


async def arkicontractor(node: ArkitektNodeFragment, actor: Actor) -> RPCContract:
    arkitekt_node = await afind(hash=node.hash)

    return arkiuse(
        binds=ReserveBindsInput(
            clients=node.binds.clients, templates=node.binds.templates
        )
        if node.binds
        else None,
        definition=arkitekt_node,
        postman=get_current_postman(),
        structure_registry=get_current_structure_registry(),
        provision=actor.provision.guardian,
        shrink_inputs=False,
        expand_outputs=False,
        reference=node.id,
        state_hook=actor.on_reservation_change,
    )  # No need to shrink inputs/outsputs for arkicontractors


async def localcontractor(node: LocalNodeFragment, actor: Actor) -> RPCContract:
    print("Creating local contractor node hash: ", node)
    return localuse(
        hash=node.hash,
        agent=actor.agent,
        structure_registry=get_current_structure_registry(),
        shrink_inputs=False,
        expand_outputs=False,
        reference=node.id,
        provision=actor.provision.provision,
    )  # No need to shrink inputs/outputs for arkicontractors


async def arkimockcontractor(node: ArkitektNodeFragment, actor: Actor) -> RPCContract:
    return mockuse(
        node=node,
        provision=actor.provision.guardian,
        shrink_inputs=False,
        shrink_outputs=False,
    )  # No need to shrink inputs/outputs for arkicontractors
