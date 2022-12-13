import json
import pytest
from fluss.api.schema import (
    ArkitektNodeFragment,
    FlowNodeFragmentBaseArkitektNode,
)
from rekuest.api.schema import NodeKind
import yaml
from .utils import build_relative, expectnext
from rekuest.messages import Provision, Assignation
from rekuest.agents.transport.protocols.agent_json import *
from reaktion.actor import FlowActor
from rekuest.agents.transport.mock import MockAgentTransport
from .flows import add_three_flow
from rekuest.postmans.utils import mockuse
from reaktion.atoms.arkitekt import ArkitektMapAtom
import asyncio
from reaktion.events import InEvent, OutEvent, EventType
from reaktion.atoms.transport import AtomTransport
from .nodes import arkitekt_functional_node


async def mockcontractor(node: ArkitektNodeFragment, provision: Provision):

    return mockuse(
        returns=[streamitem.mock() for streamitem in node.outstream[0]],
        reserve_sleep=0.1,
        assign_sleep=0.1,
    )


@pytest.mark.asyncio
@pytest.mark.actor
async def test_arkitekt_atom(
    arkitekt_functional_node: FlowNodeFragmentBaseArkitektNode,
):

    # This parts simpulates the provision

    provision = Provision(provision=1, guardian=1, user=1)

    contract = await mockcontractor(arkitekt_functional_node, provision)
    await contract.aenter()  # We need to enter the context manager

    # This part simulates the assignation

    event_queue = asyncio.Queue()
    atomtransport = AtomTransport(queue=event_queue)

    assignation = Assignation(assignation=1, user=1, provision=1, args=[])

    async with ArkitektMapAtom(
        node=arkitekt_functional_node,
        contract=contract,
        transport=atomtransport,
        assignation=assignation,
    ) as atom:
        task = asyncio.create_task(atom.start())
        await asyncio.sleep(0.1)

        await atom.put(
            InEvent(target=1, handle="arg_1", type=EventType.NEXT, value=(1,))
        )
        answer = await atomtransport.get()
        expectnext(answer)

        task.cancel()
        try:
            await asyncio.wait_for(task, timeout=0.1)
        except asyncio.CancelledError:
            pass
