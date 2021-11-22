from arkitekt.messages.postman.assign.assign_log import AssignLogMessage
from arkitekt.messages.postman.assign.bounced_assign import BouncedAssignMessage
from arkitekt.messages.postman.log import LogLevel
from arkitekt.messages.postman.assign.assign_critical import AssignCriticalMessage
from arkitekt.messages.postman.provide.bounced_provide import BouncedProvideMessage
from reaktion.events import (
    CancelEvent,
    DoneEvent,
    ErrorEvent,
    EventType,
    ReturnEvent,
    SkipEvent,
    TransitionEvent,
    YieldEvent,
)
from arkitekt.messages.postman.assign.assign_done import AssignDoneMessage
from arkitekt.messages.postman.assign.assign_yield import AssignYieldsMessage
from arkitekt.messages.postman.assign.assign_cancelled import AssignCancelledMessage
from arkitekt.messages.postman.assign.assign_return import AssignReturnMessage
from arkitekt.schema.enums import NodeType
from arkitekt.packers.utils import expand_inputs
from arkitekt.messages.postman.assign.bounced_forwarded_assign import (
    BouncedForwardedAssignMessage,
)
from arkitekt.messages.postman.reserve.reserve_transition import ReserveState
from arkitekt.schema import Node
from reaktion.engine import Engine
from typing import Dict
from fluss.schema import Graph
from arkitekt.actors.base import Actor
from arkitekt.contracts.reservation import Reservation
from fluss import diagram
from arkitekt.threadvars import assign_message
import asyncio
import logging
import json

logger = logging.getLogger(__name__)


class ReactiveGraphActor(Actor):
    async def on_reservation_transition(
        self, diagramID: str, reservation: Reservation, state: ReserveState
    ):
        await self.provide_log(
            f"Event: {json.dumps(TransitionEvent(diagram_id=diagramID, reservation=reservation.reference, state=state, message='Caused by a Transition').dict())}"
        )

    async def on_provide(self, provision: BouncedProvideMessage):
        self.graph = await Graph.asyncs.get(template=self.template)
        self.node = await Node.asyncs.get(template=self.template)
        self.engine = Engine(self.graph.diagram)

        self.diagramNodeIDReservationMap = await self.engine.generateReservationsMap(
            provision.meta.reference,
            provision.meta.context,
            self.on_reservation_transition,
        )
        logger.info(self.diagramNodeIDReservationMap)
        # Now we can start the reservations

    async def on_unprovide(self, provision: BouncedProvideMessage):
        ending_futures = [
            res.end() for id, res in self.diagramNodeIDReservationMap.items()
        ]
        cancelled_tasks = await asyncio.gather(*ending_futures, return_exceptions=True)

        for i in cancelled_tasks:
            if i is None:
                logger.info("Wonderfuly shutdown")
            else:
                logger.error("Shit is going down the drains!")

        logger.info(self.diagramNodeIDReservationMap)

    async def assign_log(
        self,
        assign: BouncedAssignMessage,
        message: str,
        level: LogLevel = LogLevel.INFO,
    ):
        message = AssignLogMessage(
            data={"message": str(message), "level": level},
            meta={**assign.meta.dict(exclude={"type"})},
        )
        await self.transport.forward(message)

    async def assign_generator(self, args, kwargs, assign):
        assert len(self.engine.argNode.data.args) == len(
            args
        ), "Received different arguments then our Engines ArgNode requires"
        nodeIdConstantsMap = self.engine.generateConstantsMap(kwargs)
        logger.info(nodeIdConstantsMap)

        outQueue = asyncio.Queue()
        loop = asyncio.get_event_loop()

        async def log(message, level: LogLevel = LogLevel.INFO):
            await self.assign_log(assign, message, level=level)

        nodeIDAtomsMap = await self.engine.generateAtoms(
            outQueue, nodeIdConstantsMap, self.diagramNodeIDReservationMap, log=log
        )

        tasks = []
        for id, atom in nodeIDAtomsMap.items():
            tasks.append(loop.create_task(atom.run()))

        await outQueue.put(
            ReturnEvent(
                diagram_id=self.engine.argNode.id, handle="returns", returns=args
            )
        )
        await outQueue.put(
            DoneEvent(diagram_id=self.engine.argNode.id, handle="returns")
        )
        initial_nodes = self.engine.getInitialNodes()
        for diagramNode in initial_nodes:
            await nodeIDAtomsMap[diagramNode.id].assign_handle(
                "args",
                ReturnEvent(
                    diagram_id=self.engine.argNode.id, handle="returns", returns=[]
                ),
            )
            await nodeIDAtomsMap[diagramNode.id].assign_handle(
                "args", DoneEvent(diagram_id=self.engine.argNode.id, handle="returns")
            )

        try:
            while True:
                event: EventType = await outQueue.get()
                await log(f"Event: {json.dumps(event.dict())}")
                logger.info(f"Assingining {event} from Mainloop")

                handle_nodes = self.engine.connectedNodesWithHandle(
                    event.diagram_id, event.handle
                )

                for handle, diagramNode in handle_nodes:
                    logger.info(f" ...  to {handle} on {diagramNode}")

                    if isinstance(event, YieldEvent) or isinstance(event, ReturnEvent):
                        if isinstance(diagramNode, diagram.ReturnNode):
                            await log(
                                f"Event: {json.dumps(YieldEvent(diagram_id=diagramNode.id, returns=event.returns, handle='returns').dict())}"
                            )
                            yield event.returns
                        else:
                            await nodeIDAtomsMap[diagramNode.id].assign_handle(
                                handle, event
                            )

                    # TODO: Handle SKip events

                    if isinstance(event, DoneEvent):
                        if isinstance(diagramNode, diagram.ReturnNode):
                            await log(
                                f"Event: {json.dumps(DoneEvent(diagram_id=diagramNode.id, handle='returns').dict())}"
                            )
                            return
                        else:
                            await nodeIDAtomsMap[diagramNode.id].assign_handle(
                                handle, event
                            )

                    if isinstance(event, ErrorEvent):
                        if isinstance(diagramNode, diagram.ReturnNode):
                            await log(
                                f"Event: {json.dumps(ErrorEvent(diagram_id=diagramNode.id, exception=event.exception, message=event.message, throwing=event.throwing, handle='returns').dict())}"
                            )
                            raise type(event.exception, (Exception,), {})(event.message)
                        else:
                            await nodeIDAtomsMap[diagramNode.id].assign_handle(
                                handle, event
                            )

        except asyncio.CancelledError as e:
            logger.error("Received Cancellation babe")

            await log(
                f"Event: {json.dumps(CancelEvent(diagram_id=self.engine.argNode.id,handle='args').dict())}"
            )
            await log(
                f"Event: {json.dumps(CancelEvent(diagram_id=self.engine.kwargNode.id,handle='args').dict())}"
            )
            await log(
                f"Event: {json.dumps(CancelEvent(diagram_id=self.engine.returnNode.id,handle='args').dict())}"
            )

            for task in tasks:
                task.cancel()

            cancelled_tasks = await asyncio.gather(*tasks, return_exceptions=True)
            for i in cancelled_tasks:
                if isinstance(i, CancelEvent):
                    await log(f"Event: {json.dumps(i.dict())}")
                else:
                    logger.error("Shit is going down the drains!")

            raise e

        except Exception as e:

            for task in tasks:
                task.cancel()

            cancelled_tasks = await asyncio.gather(*tasks, return_exceptions=True)

            for i in cancelled_tasks:
                if isinstance(i, CancelEvent):
                    await log(f"Event: {json.dumps(i.dict())}")
                else:
                    print("Shit is going down the drains!")

            raise e

    async def assign_function(self, args, kwargs, assign):
        assert len(self.engine.argNode.data.args) == len(
            args
        ), "Received different arguments then our Engines ArgNode requires"
        nodeIdConstantsMap = self.engine.generateConstantsMap(kwargs)
        logger.info(nodeIdConstantsMap)

        outQueue = asyncio.Queue()
        loop = asyncio.get_event_loop()

        async def log(message, level: LogLevel = LogLevel.INFO):
            await self.assign_log(assign, message, level=level)

        nodeIDAtomsMap = await self.engine.generateAtoms(
            outQueue, nodeIdConstantsMap, self.diagramNodeIDReservationMap, log=log
        )

        tasks = []
        for id, atom in nodeIDAtomsMap.items():
            tasks.append(loop.create_task(atom.run()))

        await outQueue.put(
            ReturnEvent(
                diagram_id=self.engine.argNode.id, handle="returns", returns=args
            )
        )
        await outQueue.put(
            DoneEvent(diagram_id=self.engine.argNode.id, handle="returns")
        )
        initial_nodes = self.engine.getInitialNodes()
        for diagramNode in initial_nodes:
            await nodeIDAtomsMap[diagramNode.id].assign_handle(
                "args",
                ReturnEvent(
                    diagram_id=self.engine.argNode.id, handle="returns", returns=[]
                ),
            )
            await nodeIDAtomsMap[diagramNode.id].assign_handle(
                "args", DoneEvent(diagram_id=self.engine.argNode.id, handle="returns")
            )

        saved_returns = None

        try:
            while True:
                event: EventType = await outQueue.get()
                await log(f"Event: {json.dumps(event.dict())}")
                logger.info(f"Assingining {event} from Mainloop")

                handle_nodes = self.engine.connectedNodesWithHandle(
                    event.diagram_id, event.handle
                )

                for handle, diagramNode in handle_nodes:
                    logger.info(f" ...  to {handle} on {diagramNode}")

                    if isinstance(event, YieldEvent) or isinstance(event, ReturnEvent):
                        if isinstance(diagramNode, diagram.ReturnNode):
                            await log(
                                f"Event: {json.dumps(YieldEvent(diagram_id=diagramNode.id, returns=event.returns, handle='returns').dict())}"
                            )

                            if saved_returns is not None:
                                await log(
                                    "We have received another event on the output (probably because there is a Generator, we are omitting the old result"
                                )

                            saved_returns = event.returns
                        else:
                            await nodeIDAtomsMap[diagramNode.id].assign_handle(
                                handle, event
                            )

                    # TODO: Handle SKip events

                    if isinstance(event, DoneEvent):
                        if isinstance(diagramNode, diagram.ReturnNode):
                            await log(
                                f"Event: {json.dumps(DoneEvent(diagram_id=diagramNode.id, handle='returns').dict())}"
                            )
                            assert (
                                saved_returns is not None
                            ), "We received a Done event before a yield event"
                            return saved_returns
                        else:
                            await nodeIDAtomsMap[diagramNode.id].assign_handle(
                                handle, event
                            )

                    if isinstance(event, ErrorEvent):
                        if isinstance(diagramNode, diagram.ReturnNode):
                            await log(
                                f"Event: {json.dumps(ErrorEvent(diagram_id=diagramNode.id, exception=event.exception, message=event.message, throwing=event.throwing, handle='returns').dict())}"
                            )
                            raise type(event.exception, (Exception,), {})(event.message)
                        else:
                            await nodeIDAtomsMap[diagramNode.id].assign_handle(
                                handle, event
                            )

        except asyncio.CancelledError as e:
            logger.error("Received Cancellation babe")

            await log(
                f"Event: {json.dumps(CancelEvent(diagram_id=self.engine.argNode.id,handle='args').dict())}"
            )
            await log(
                f"Event: {json.dumps(CancelEvent(diagram_id=self.engine.kwargNode.id,handle='args').dict())}"
            )
            await log(
                f"Event: {json.dumps(CancelEvent(diagram_id=self.engine.returnNode.id,handle='args').dict())}"
            )

            for task in tasks:
                task.cancel()

            cancelled_tasks = await asyncio.gather(*tasks, return_exceptions=True)
            for i in cancelled_tasks:
                if isinstance(i, CancelEvent):
                    await log(f"Event: {json.dumps(i.dict())}")
                else:
                    logger.error("Shit is going down the drains!")

            raise e

        except Exception as e:

            for task in tasks:
                task.cancel()

            cancelled_tasks = await asyncio.gather(*tasks, return_exceptions=True)

            for i in cancelled_tasks:
                if isinstance(i, CancelEvent):
                    await log(f"Event: {json.dumps(i.dict())}")
                else:
                    print("Shit is going down the drains!")

            raise e

    async def on_assign(self, message: BouncedForwardedAssignMessage):
        try:
            logger.info(f"Assignment received {message}")
            args, kwargs = (
                message.data.args,
                message.data.kwargs,
            )  # We are not expanding the inputs as they are just passed on

            assign_message.set(message)

            if self.node.type == NodeType.FUNCTION:
                returns = await self.assign_function(args, kwargs, message)
                assign_message.set(None)

                await self.transport.forward(
                    AssignReturnMessage(
                        data={
                            "returns": returns
                            or []  # We are not shrinking the outputs as they are already shrinked
                        },
                        meta={
                            "reference": message.meta.reference,
                            "extensions": message.meta.extensions,
                        },
                    )
                )

            if self.node.type == NodeType.GENERATOR:
                async for returns in self.assign_generator(args, kwargs, message):
                    await self.transport.forward(
                        AssignYieldsMessage(
                            data={"returns": returns or []},
                            meta={
                                "reference": message.meta.reference,
                                "extensions": message.meta.extensions,
                            },
                        )
                    )

                assign_message.set(None)

                await self.transport.forward(
                    AssignDoneMessage(
                        data={"returns": None},
                        meta={
                            "reference": message.meta.reference,
                            "extensions": message.meta.extensions,
                        },
                    )
                )

        except asyncio.CancelledError as e:

            await self.transport.forward(
                AssignCancelledMessage(
                    data={"canceller": str(e)},
                    meta={
                        "reference": message.meta.reference,
                        "extensions": message.meta.extensions,
                    },
                )
            )

        except Exception as e:
            logger.exception(e)
            await self.transport.forward(
                AssignCriticalMessage(
                    data={"type": e.__class__.__name__, "message": str(e)},
                    meta={
                        "reference": message.meta.reference,
                        "extensions": message.meta.extensions,
                    },
                )
            )

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.diagramNodeReservation: Dict[
            diagram.ArkitektNode.id, Reservation
        ] = {}  # A map of
