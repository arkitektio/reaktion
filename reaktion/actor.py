import asyncio
import logging
from typing import Dict

from pydantic import BaseModel, Field
from arkitekt.actors.base import Actor
from arkitekt.actors.functional import AsyncFuncActor
from arkitekt.api.schema import (
    AssignationLogLevel,
    AssignationStatus,
    ProvisionFragment,
    TemplateFragment,
    afind,
)
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
    RunMutationStart,
    aget_flow,
    arun,
    arunlog,
    asnapshot,
)
from reaktion.events import EventType, OutEvent
from reaktion.utils import connected_events
from reaktion.atoms.utils import atomify


class NodeState(BaseModel):
    latestevent: OutEvent


class FlowFuncActor(AsyncFuncActor):
    contracts: Dict[str, ReservationContract] = Field(default_factory=dict)
    flow: Contextual[FlowFragment]
    expand_inputs: bool = False
    shrink_outputs: bool = False

    run_states: Dict[
        str,
        Dict[str, NodeState],
    ] = Field(default_factory=dict)

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

        self.contracts = {key: use(node=value) for key, value in instances.items()}

        await self.aprov_log("Entering")

        for contract in self.contracts.values():
            await contract.aenter()

        await self.aprov_log("Started")

    async def update_state(self, run: RunMutationStart, event: OutEvent):
        if run.id not in self.run_states:
            self.run_states[run.id] = {}

        self.run_states[run.id][event.source] = event.to_state()

    async def push_state(self, run: RunMutationStart):
        print(self.run_states[run.id])
        try:
            await asnapshot(run, self.run_states[run.id])

        except:
            logging.exception("Failed to push state", exc_info=True)
        print("OINAOSINDOASINDOASINDOASINDOISNADOASNd")

    async def on_assign(self, assignation: Assignation):

        run = await arun(assignation=assignation.id, flow=self.flow)

        await self.aass_log(assignation.assignation, "Starting")
        try:
            await self.transport.change_assignation(
                assignation.assignation,
                status=AssignationStatus.ASSIGNED,
            )

            event_queue = asyncio.Queue()

            argNode = [
                x for x in self.flow.graph.nodes if isinstance(x, ArgNodeFragment)
            ][0]
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

            await self.aass_log(assignation.assignation, "Set up the graph")

            async def ass_log(assignation, level, message):
                await self.aass_log(assignation.assignation, level, message)
                logging.info(f"{assignation}, {message}")

            atoms = {
                x.id: atomify(x, event_queue, self.contracts, alog=ass_log)
                for x in participatingNodes
            }

            await self.aass_log(assignation.assignation, "Atomification complete")

            tasks = [asyncio.create_task(atom.run()) for atom in atoms.values()]

            initial_event = OutEvent(
                handle="return_0",
                type=EventType.NEXT,
                source=argNode.id,
                value=assignation.args,
            )
            initial_done_event = OutEvent(
                handle="return_0", type=EventType.COMPLETE, source=argNode.id
            )

            await event_queue.put(initial_event)
            await event_queue.put(initial_done_event)
            print("Starting Workflow")

            complete = False
            i = 0

            while not complete:
                event = await event_queue.get()
                await self.aass_log(assignation.assignation, f"Received Event {event}")
                await self.update_state(run, event)

                if self.flow.brittle:
                    print("FLOOOWSS BRITTTLEEE MAN")
                    if event.type == EventType.ERROR:
                        raise event.value

                if i == 2:
                    await self.push_state(run)
                    i = 0

                spawned_events = connected_events(self.flow.graph, event)

                for spawned_event in spawned_events:
                    print("->", spawned_event)

                    if spawned_event.target == returnNode.id:

                        if spawned_event.type == EventType.NEXT:
                            print("Setting result")
                            returns = spawned_event.value
                            continue

                        if spawned_event.type == EventType.ERROR:
                            raise spawned_event.value

                        if spawned_event.type == EventType.COMPLETE:
                            print("Going out?")
                            complete = True
                            continue

                    assert (
                        spawned_event.target in atoms
                    ), "Unknown target. Your flow is connected wrong"
                    if spawned_event.target in atoms:
                        await atoms[spawned_event.target].put(spawned_event)

            for tasks in tasks:
                tasks.cancel()

            await asyncio.gather(*tasks)

            await self.aass_log(assignation.assignation, "Finished")
            await self.transport.change_assignation(
                assignation.assignation,
                status=AssignationStatus.RETURNED,
                returns=returns,
            )

        except Exception as e:

            await self.push_state(run)
            await self.aass_log(
                assignation.assignation, message=repr(e), level=AssignationStatus.ERROR
            )
            await self.transport.change_assignation(
                assignation.assignation,
                status=AssignationStatus.CRITICAL,
                message=repr(e),
            )

    async def on_unprovide(self):

        for contract in self.contracts.values():
            await contract.adisconnect()
