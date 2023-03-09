import asyncio
import logging
from typing import Callable, Dict, Optional, Protocol, runtime_checkable

from pydantic import BaseModel, Field

from fluss.api.schema import (ArgNodeFragment, ArkitektNodeFragment,
                              FlowFragment, KwargNodeFragment,
                              ReactiveNodeFragment, ReturnNodeFragment,
                              RunMutationStart, aget_flow, arun, arunlog,
                              asnapshot, atrack)
from koil.types import Contextual
from reaktion.atoms.transport import AtomTransport
from reaktion.atoms.utils import atomify
from reaktion.contractors import (NodeContractor, arkicontractor,
                                  localcontractor)
from reaktion.events import EventType, InEvent, OutEvent
from reaktion.utils import connected_events
from rekuest.actors.base import Actor
from rekuest.actors.functional import AsyncFuncActor, AsyncGenActor
from rekuest.api.schema import (AssignationLogLevel, AssignationStatus,
                                NodeFragment, NodeKind, ProvisionFragment,
                                ProvisionStatus, ReservationFragment,
                                ReservationStatus, TemplateFragment, afind,
                                aget_template)
from rekuest.messages import Assignation, Provision
from rekuest.postmans.utils import (ReservationContract, RPCContract, arkiuse,
                                    localuse)

logger = logging.getLogger(__name__)


class NodeState(BaseModel):
    latestevent: OutEvent


class FlowActor(Actor):
    is_generator: bool = False
    flow: FlowFragment
    contracts: Dict[str, RPCContract] = Field(default_factory=dict)
    expand_inputs: bool = False
    shrink_outputs: bool = False
    provided = False
    is_generator: bool = False
    nodeContractor: NodeContractor = arkicontractor

    # Functionality for running the flow

    # Assign Related Functionality
    run_mutation: Callable = arun
    snapshot_mutation: Callable = asnapshot
    track_mutation: Callable = atrack

    atomifier: Callable = atomify
    """ Atomifier is a function that takes a node and returns an atom """

    run_states: Dict[
        str,
        Dict[str, NodeState],
    ] = Field(default_factory=dict)

    reservation_state: Dict[str, ReservationFragment] = Field(default_factory=dict)
    _lock = None

    async def on_provide(self, provision: Provision):
        self._lock = asyncio.Lock()

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

        self.contracts = {
            node.id: await self.nodeContractor(node, self)
            for node in arkitektNodes
        }

        futures = [contract.aenter() for contract in self.contracts.values()]
        await asyncio.gather(*futures)

        self.provided = True
        await self.on_reservation_change(None)

    async def on_reservation_change(self, status: ReservationStatus):
        async with self._lock:
            unactive_reservations = [
                res
                for res in self.contracts.values()
                if res.state != ReservationStatus.ACTIVE
            ]
            if self.provided:
                if len(unactive_reservations) > 0:
                    await self.transport.change_provision(
                        self.provision.provision,
                        status=ProvisionStatus.CRITICAL,
                    )
                else:
                    await self.transport.change_provision(
                        self.provision.provision,
                        status=ProvisionStatus.ACTIVE,
                    )

    async def on_assign(self, assignation: Assignation):

        run = await self.run_mutation(
            assignation=assignation.assignation, flow=self.flow
        )

        await self.aass_log(assignation.assignation, "Starting")

        t = 0
        state = {}
        await self.snapshot_mutation(run=run, events=list(state.values()), t=t)

        try:
            event_queue = asyncio.Queue()

            atomtransport = AtomTransport(queue=event_queue)

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

            async def ass_log(assignation: Assignation, level, message):
                await self.aass_log(assignation.assignation, level, message)
                logging.info(f"{assignation}, {message}")

            atoms = {
                x.id: self.atomifier(
                    x, atomtransport, self.contracts, assignation, alog=ass_log
                )
                for x in participatingNodes
            }

            await self.aass_log(assignation.assignation, "Atomification complete")

            await asyncio.gather(*[atom.aenter() for atom in atoms.values()])

            tasks = [asyncio.create_task(atom.start()) for atom in atoms.values()]




            stream = argNode.outstream[0]
            value = [assignation.args[key] for key, index in enumerate(stream)]

            initial_event = OutEvent(
                handle=f"return_0",
                type=EventType.NEXT,
                source=argNode.id,
                value=value,
            )
            initial_done_event = OutEvent(
                handle=f"return_0",
                type=EventType.COMPLETE,
                source=argNode.id,
            )

            logger.info(f"Putting initial event {initial_event}")

            await event_queue.put(initial_event)
            await event_queue.put(initial_done_event)




            edge_targets = [e.target for e in self.flow.graph.edges]
            nodes_without_instream = [x for x in participatingNodes if len(x.instream[0]) == 0 and x.id not in edge_targets]


            logger.error(f"Nodes without instream: {nodes_without_instream}")
            for node in nodes_without_instream:

                assert node.id in atoms, "Atom not found. Should not happen."
                atom = atoms[node.id]

                initial_event = InEvent(
                        target=node.id,
                        handle="arg_0",
                        type=EventType.NEXT,
                        value=[],
                )
                done_event = InEvent(
                        target=node.id,
                        handle="arg_0",
                        type=EventType.COMPLETE,
                )

                await atom.put(initial_event)
                await atom.put(done_event)



            complete = False

            returns = []

            while not complete:
                event: OutEvent = await event_queue.get()
                event_queue.task_done()

                if self.flow.brittle:
                    if event.type == EventType.ERROR:
                        raise event.value

                track = await self.track_mutation(
                    run=run,
                    source=event.source,
                    handle=event.handle,
                    value=event.value
                    if event.value and not isinstance(event.value, Exception)
                    else str(event.value),
                    type=event.type,
                    t=t,
                )
                state[event.source] = track.id

                t += 1

                if t % 3 == 0:
                    await self.snapshot_mutation(
                        run=run, events=list(state.values()), t=t
                    )

                spawned_events = connected_events(self.flow.graph, event)
                if not spawned_events:
                    logger.warning(f"No events spawned from {event}")

                for spawned_event in spawned_events:
                    logger.info(f"-> {spawned_event}")

                    if spawned_event.target == returnNode.id:

                        if spawned_event.type == EventType.NEXT:
                            returns = spawned_event.value
                            if self.is_generator:
                                await self.transport.change_assignation(
                                    assignation.assignation,
                                    status=AssignationStatus.YIELD,
                                    returns=returns,
                                )

                        if spawned_event.type == EventType.ERROR:
                            raise spawned_event.value

                        if spawned_event.type == EventType.COMPLETE:
                            complete = True
                            if not self.is_generator:
                                await self.transport.change_assignation(
                                    assignation.assignation,
                                    status=AssignationStatus.RETURNED,
                                    returns=returns,
                                )
                            else:
                                await self.transport.change_assignation(
                                    assignation.assignation,
                                    status=AssignationStatus.DONE,
                                )

                    else:
                        assert (
                            spawned_event.target in atoms
                        ), "Unknown target. Your flow is connected wrong"
                        if spawned_event.target in atoms:
                            await atoms[spawned_event.target].put(spawned_event)

            for task in tasks:
                task.cancel()

            await asyncio.gather(*tasks, return_exceptions=True)

        except asyncio.CancelledError as e:

            for task in tasks:
                task.cancel()

            try:
                await asyncio.wait_for(
                    asyncio.gather(*tasks, return_exceptions=True), timeout=4
                )
            except asyncio.TimeoutError:
                pass

            await self.transport.change_assignation(
                assignation.assignation, status=AssignationStatus.CANCELLED
            )

        except Exception as e:
            logging.critical(f"Assignation {assignation} failed", exc_info=True)

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
            await contract.aexit()
