from typing import Protocol, runtime_checkable
from rekuest.postmans.utils import RPCContract, arkiuse, localuse, mockuse
from fluss.api.schema import ArkitektNodeFragment
from rekuest.messages import Provision
from rekuest.api.schema import afind
from rekuest.postmans.vars import get_current_postman
from rekuest.structures.registry import get_current_structure_registry


@runtime_checkable
class NodeContractor(Protocol):
    async def __call__(
        self, node: ArkitektNodeFragment, provision: Provision
    ) -> RPCContract:
        ...


async def arkicontractor(
    node: ArkitektNodeFragment, provision: Provision
) -> RPCContract:

    node = await afind(hash=node.hash)

    return arkiuse(
        definition=node,
        postman=get_current_postman(),
        structure_registry=get_current_structure_registry(),
        provision=provision.guardian,
        shrink_inputs=False,
        expand_outputs=False,
    )  # No need to shrink inputs/outputs for arkicontractors


async def localcontractor(
    template: ArkitektNodeFragment, provision: Provision
) -> RPCContract:

    return localuse(
        template=template,
        provision=provision.guardian,
        shrink_inputs=False,
        shrink_outputs=False,
    )  # No need to shrink inputs/outputs for arkicontractors


async def arkimockcontractor(
    node: ArkitektNodeFragment, provision: Provision
) -> RPCContract:

    return mockuse(
        node=node,
        provision=provision.guardian,
        shrink_inputs=False,
        shrink_outputs=False,
    )  # No need to shrink inputs/outputs for arkicontractors
