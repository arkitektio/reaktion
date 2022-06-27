import asyncio
import yaml
from arkitekt.api.schema import NodeType
from arkitekt.postmans.utils import mockuse
from arkitekt.traits.node import Reserve

from fluss.api.schema import (
    ArgNodeFragment,
    ArkitektNodeFragment,
    FlowFragmentGraph,
    KwargNodeFragment,
    ReactiveNodeFragment,
    ReturnNodeFragment,
)
from reaktion.atoms.utils import atomify
from reaktion.events import EventType, OutEvent
from reaktion.utils import connected_events
from .utils import build_relative
from reaktion.atoms.arkitekt import ArkitektMapAtom
import logging


class MockReserve(Reserve):
    package: str
    interface: str
    type: NodeType


def test_create_flow():

    with open(build_relative("flows/get_flow.yaml"), "r") as f:
        g = yaml.safe_load(f)

    t = FlowFragmentGraph(**g)


async def res_log(reservation, level, message):
    logging.info(f"{reservation}, {message}")


async def ass_log(assignation, level, message):
    logging.info(f"{assignation}, {message}")


async def test_parse_flow():

    with open(build_relative("flows/get_flow.yaml"), "r") as f:
        g = yaml.safe_load(f)

    t = FlowFragmentGraph.from_file(build_relative("flows/get_flow.yaml"))

    participating_node = [
        n
        for n in t.nodes
        if isinstance(n, ArkitektNodeFragment) or isinstance(n, ReactiveNodeFragment)
    ]
    anodes = [n for n in participating_node if isinstance(n, ArkitektNodeFragment)]
    argNode = [x for x in t.nodes if isinstance(x, ArgNodeFragment)][0]
    kwargNode = [x for x in t.nodes if isinstance(x, KwargNodeFragment)][0]
    returnNode = [x for x in t.nodes if isinstance(x, ReturnNodeFragment)][0]

    edges = t.connected_edges(anodes[0])
    assert edges, "Should have connected edges"

    event_queue = asyncio.Queue()

    reservations = {
        n.id: mockuse(
            node=MockReserve(package=n.package, type=n.kind, interface=n.interface),
            res_log=res_log,
        )
        for n in anodes
    }

    for contract in reservations.values():
        await contract.aenter()

    atoms = {
        n.id: atomify(n, event_queue, reservations, alog=ass_log)
        for n in participating_node
    }

    tasks = [asyncio.create_task(atom.run()) for atom in atoms.values()]

    initial_event = OutEvent(
        handle="return_0", type=EventType.NEXT, source=argNode.id, value=(1, 2)
    )
    initial_done_event = OutEvent(
        handle="return_0", type=EventType.COMPLETE, source=argNode.id
    )

    await event_queue.put(initial_event)
    await event_queue.put(initial_done_event)
    print("Starting Workflow")

    not_complete = True

    while not_complete:
        event = await event_queue.get()
        print("Received event", event)
        spawned_events = connected_events(t, event)

        for spawned_event in spawned_events:
            print("->", spawned_event)

            if spawned_event.target == returnNode.id:

                if spawned_event.type == EventType.NEXT:
                    print("Setting result")
                    result = spawned_event.value
                    continue

                if spawned_event.type == EventType.ERROR:
                    raise spawned_event.value

                if spawned_event.type == EventType.COMPLETE:
                    print("Going out?")
                    not_complete = False
                    continue

            assert (
                spawned_event.target in atoms
            ), "Unknown target. Your flow is connected wrong"
            if spawned_event.target in atoms:
                await atoms[spawned_event.target].put(spawned_event)

    for tasks in tasks:
        tasks.cancel()

    await asyncio.gather(*tasks)

    for contract in reservations.values():
        await contract.aexit()

    assert result == (1, 2, 1, 2), "Did not give the expected Result"
