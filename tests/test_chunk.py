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
    reactive_chunk_node,
    reactive_chunk_node_with_defaults,
)
from reaktion.atoms.transformation.chunk import ChunkAtom


@pytest.mark.asyncio
@pytest.mark.actor
async def test_chunk_without_defaults(
    reactive_chunk_node: FlowNodeFragmentBaseReactiveNode,
):

    event_queue = asyncio.Queue()
    atomtransport = MockTransport(queue=event_queue)

    assignation = Assignation(assignation=1, user=1, provision=1, args=[])

    async with ChunkAtom(
        node=reactive_chunk_node,
        transport=atomtransport,
        assignation=assignation,
    ) as atom:
        task = asyncio.create_task(atom.start())
        await asyncio.sleep(0.1)

        await atom.put(
            InEvent(
                target=atom.node.id,
                handle="arg_0",
                type=EventType.NEXT,
                value=([1, 2, 3],),
            )
        )

        answer = await atomtransport.get(timeout=0.1)
        expectnext(answer)
        assert answer.value == (1,)

        answer = await atomtransport.get(timeout=0.1)
        expectnext(answer)
        assert answer.value == (2,)

        answer = await atomtransport.get(timeout=0.1)
        expectnext(answer)
        assert answer.value == (3,)

        task.cancel()
        try:
            await asyncio.wait_for(task, timeout=0.1)
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
@pytest.mark.actor
async def test_chunk_with_sleep_defaults(
    reactive_chunk_node_with_defaults: FlowNodeFragmentBaseReactiveNode,
):

    event_queue = asyncio.Queue()
    atomtransport = MockTransport(queue=event_queue)

    assignation = Assignation(assignation=1, user=1, provision=1, args=[])
    assert reactive_chunk_node_with_defaults.defaults == {"sleep": 1000}

    async with ChunkAtom(
        node=reactive_chunk_node_with_defaults,
        transport=atomtransport,
        assignation=assignation,
    ) as atom:
        task = asyncio.create_task(atom.start())
        await asyncio.sleep(0.1)

        await atom.put(
            InEvent(
                target=atom.node.id,
                handle="arg_0",
                type=EventType.NEXT,
                value=([1, 2, 3],),
            )
        )

        answer = await atomtransport.get(timeout=1.1)
        expectnext(answer)
        assert answer.value == (1,)

        with pytest.raises(asyncio.TimeoutError):
            await atomtransport.get(
                timeout=0.1
            )  # this should timeout because we sleep for one second

        answer = await atomtransport.get(timeout=1.1)
        expectnext(answer)
        assert answer.value == (2,)

        answer = await atomtransport.get(timeout=1.1)
        expectnext(answer)
        assert answer.value == (3,)

        task.cancel()
        try:
            await asyncio.wait_for(task, timeout=0.1)
        except asyncio.CancelledError:
            pass
