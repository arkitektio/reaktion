from typing import Protocol, runtime_checkable
from rekuest.postmans.utils import RPCContract, arkiuse, localuse, mockuse
from fluss.api.schema import ArkitektNodeFragment, TemplateNodeFragment
from rekuest.messages import Provision


@runtime_checkable
class NodeContractor(Protocol):
    async def __call__(
        self, node: ArkitektNodeFragment, provision: Provision
    ) -> RPCContract:
        ...


@runtime_checkable
class TemplateContractor(Protocol):
    async def __call__(
        self, template: TemplateNodeFragment, provision: Provision
    ) -> RPCContract:
        ...


async def arkicontractor(
    node: ArkitektNodeFragment, provision: Provision
) -> RPCContract:

    return arkiuse(
        node=node,
        provision=provision.guardian,
        shrink_inputs=False,
        shrink_outputs=False,
    )  # No need to shrink inputs/outputs for arkicontractors


async def localcontractor(
    template: TemplateNodeFragment, provision: Provision
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


async def templatemockcontractor(
    node: TemplateNodeFragment, provision: Provision
) -> RPCContract:

    return mockuse(
        node=node,
        provision=provision.guardian,
        shrink_inputs=False,
        shrink_outputs=False,
    )  # No need to shrink inputs/outputs for arkicontractors
