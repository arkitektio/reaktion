from typing import Protocol, runtime_checkable
from rekuest.postmans.utils import RPCContract, arkiuse, localuse, mockuse
from fluss.api.schema import ArkitektNodeFragment, FlowNodeFragmentBaseArkitektNode
from rekuest.messages import Provision
from rekuest.api.schema import afind
from rekuest.postmans.vars import get_current_postman
from rekuest.structures.registry import get_current_structure_registry
from rekuest.actors.base import Actor

@runtime_checkable
class NodeContractor(Protocol):
    async def __call__(
        self, node: ArkitektNodeFragment, actor: Actor
    ) -> RPCContract:
        ...


async def arkicontractor(
    node: FlowNodeFragmentBaseArkitektNode, actor: Actor
) -> RPCContract:

    arkitekt_node = await afind(hash=node.hash)

    return arkiuse(
        definition=arkitekt_node,
        postman=get_current_postman(),
        structure_registry=get_current_structure_registry(),
        provision=actor.provision.guardian,
        shrink_inputs=False,
        expand_outputs=False,
        reference=node.id,
        state_hook=actor.on_reservation_change,
    )  # No need to shrink inputs/outsputs for arkicontractors


async def localcontractor(
    template: ArkitektNodeFragment, actor: Actor
) -> RPCContract:

    return localuse(
        template=template,
        provision=actor.provision.guardian,
        shrink_inputs=False,
        shrink_outputs=False,
    )  # No need to shrink inputs/outputs for arkicontractors


async def arkimockcontractor(
    node: ArkitektNodeFragment, actor: Actor
) -> RPCContract:

    return mockuse(
        node=node,
        provision=actor.provision.guardian,
        shrink_inputs=False,
        shrink_outputs=False,
    )  # No need to shrink inputs/outputs for arkicontractors
