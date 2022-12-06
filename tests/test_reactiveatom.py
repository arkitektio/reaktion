import json
import pytest
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
from reaktion.atoms.transport import AtomTransport, MockTransport
from .nodes import (
    reactive_zip_node,
    FlowNodeFragmentBaseReactiveNode,
    reactive_withlatest_node,
)
from reaktion.atoms.combination.zip import ZipAtom
from reaktion.atoms.combination.withlatest import WithLatestAtom


@pytest.mark.asyncio
@pytest.mark.actor
async def test_zip_atom(
    reactive_zip_node: FlowNodeFragmentBaseReactiveNode,
):

    event_queue = asyncio.Queue()
    atomtransport = MockTransport(queue=event_queue)

    assignation = Assignation(assignation=1, user=1, provision=1, args=[])

    async with ZipAtom(
        node=reactive_zip_node,
        transport=atomtransport,
        assignation=assignation,
    ) as atom:
        task = asyncio.create_task(atom.start())
        await asyncio.sleep(0.1)

        await atom.put(
            InEvent(
                target=atom.node.id, handle="arg_0", type=EventType.NEXT, value=(1,)
            )
        )
        await asyncio.sleep(0.1)
        await atom.put(
            InEvent(
                target=atom.node.id, handle="arg_1", type=EventType.NEXT, value=(2,)
            )
        )

        answer = await atomtransport.get()
        expectnext(answer)
        assert answer.value == (1, 2)

        task.cancel()
        try:
            await asyncio.wait_for(task, timeout=0.1)
        except asyncio.CancelledError:
            pass


@pytest.mark.skip
@pytest.mark.asyncio
@pytest.mark.actor
async def test_with_latest(
    reactive_withlatest_node: FlowNodeFragmentBaseReactiveNode,
):

    event_queue = asyncio.Queue()
    atomtransport = MockTransport(queue=event_queue)

    assignation = Assignation(assignation=1, user=1, provision=1, args=[])

    async with WithLatestAtom(
        node=reactive_withlatest_node,
        transport=atomtransport,
        assignation=assignation,
    ) as atom:
        task = asyncio.create_task(atom.start())
        await asyncio.sleep(0.1)

        await atom.put(
            InEvent(
                target=atom.node.id, handle="arg_0", type=EventType.NEXT, value=(1,)
            )
        )
        await asyncio.sleep(0.1)
        await atom.put(
            InEvent(
                target=atom.node.id, handle="arg_1", type=EventType.NEXT, value=(2,)
            )
        )

        answer = await atomtransport.get()
        expectnext(answer)
        assert answer.value == (1, 2)

        task.cancel()
        try:
            await asyncio.wait_for(task, timeout=0.1)
        except asyncio.CancelledError:
            pass
