import asyncio
from typing import Dict

from pydantic import Field
from arkitekt.actors.base import Actor
from arkitekt.api.schema import ProvisionFragment, TemplateFragment, afind
from arkitekt.messages import Assignation, Provision
from arkitekt.postmans.utils import ReservationContract, use
from koil.types import Contextual
from fluss.api.schema import (
    ArgNodeFragment,
    ArkitektNodeFragment,
    FlowFragment,
    KwargNodeFragment,
    ReactiveNodeFragment,
    ReturnNodeFragment,
    aget_flow,
)
from reaktion.events import EventType, OutEvent
from .utils import atomify, connected_events


class FlowActor(Actor):
    contracts: Dict[str, ReservationContract] = Field(default_factory=dict)
    flow: Contextual[FlowFragment]

    async def on_provide(self, provision: ProvisionFragment):
        self.flow = await aget_flow(id=self.provision.template.params["flow"])

        argNode = [x for x in self.flow.graph.nodes if isinstance(x, ArgNodeFragment)][
            0
        ]
        kwargNode = [
            x for x in self.flow.graph.nodes if isinstance(x, KwargNodeFragment)
        ][0]
        returnNode = [
            x for x in self.flow.graph.nodes if isinstance(x, ReturnNodeFragment)
        ][0]

        arkitektNodes = [
            x for x in self.flow.graph.nodes if isinstance(x, ArkitektNodeFragment)
        ]

        instances = {
            x.id: await afind(package=x.package, interface=x.interface)
            for x in arkitektNodes
        }

        self.contracts = {key: use(value) for key, value in instances.items()}

        for contract in self.contracts.values():
            await contract.connect()

    async def on_assign(self, assignation: Assignation):

        event_queue = asyncio.Queue()

        argNode = [x for x in self.flow.graph.nodes if isinstance(x, ArgNodeFragment)][
            0
        ]
        kwargNode = [
            x for x in self.flow.graph.nodes if isinstance(x, KwargNodeFragment)
        ][0]
        returnNode = [
            x for x in self.flow.graph.nodes if isinstance(x, ReturnNodeFragment)
        ][0]

        participatingNodes = [
            x
            for x in self.flow.graph.nodes
            if isinstance(x, ArkitektNodeFragment)
            or isinstance(x, ReactiveNodeFragment)
        ]

        atoms = {
            x.id: atomify(x, event_queue, self.contracts) for x in participatingNodes
        }
        for atom in atoms.values():
            print(atom.json(indent=4))

        tasks = [asyncio.create_task(atom.run()) for atom in atoms.values()]

        initial_event = OutEvent(
            handle="returns", type=EventType.NEXT, source=argNode.id, value=(1, 2)
        )
        initial_done_event = OutEvent(
            handle="returns", type=EventType.COMPLETE, source=argNode.id
        )

        await event_queue.put(initial_event)
        await event_queue.put(initial_done_event)
        print("Starting Workflow")

        not_complete = True

        while not_complete:
            event = await event_queue.get()
            print("Received event", event)
            spawned_events = connected_events(self.flow, event)

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

        return result

    async def on_unprovide(self):

        for contract in self.contracts.values():
            await contract.adisconnect()
