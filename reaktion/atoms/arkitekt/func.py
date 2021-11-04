from typing import Dict
from fluss.diagram import DiagramNode
from ...atoms.base import Atom
import asyncio
from arkitekt.contracts.reservation import Omitted, Reservation
from ...events import *
import logging

logger = logging.getLogger(__name__)


class FuncNoParallelArkitektAtom(Atom):
    """Non parallizable Atom, function call will not be parallized if called in row"""

    def __init__(
        self,
        diagramNode: DiagramNode,
        outQueue: asyncio.Queue,
        reservation: Reservation = None,
        constants: Dict = {},
        maxsize: int = 0,
        **kwargs,
    ) -> None:
        super().__init__(diagramNode, outQueue, constants=constants, **kwargs)
        self.reservation: Reservation = reservation
        self.timeout = 4000
        self.inqueue = asyncio.Queue(maxsize=maxsize)

    async def run(self):
        try:
            while True:
                event: EventType = await self.inqueue.get()
                logger.info(f"FUNC REVEICED {event}")

                if isinstance(event, DoneEvent):
                    await self.log("Node Upstream is done, we can now also rest")
                    await self.send_done_event("returns")
                    break

                if isinstance(event, SkipEvent):
                    await self.log("Node Upstream is done, we can now also rest")
                    await self.pass_skip_event("returns", event)
                    break

                if isinstance(event, ErrorEvent):
                    await self.log("Node Upstream is done, we can now also rest")
                    await self.pass_error_event("returns", event)
                    break

                if isinstance(event, YieldEvent) or isinstance(event, ReturnEvent):
                    await self.log(
                        f"Node Upstream yielded or returned {event.returns}. Assigning as Function"
                    )

                    try:
                        result = await asyncio.wait_for(
                            self.reservation.assign_async(
                                *event.returns,
                                **self.constants,
                                bypass_shrink=True,
                                bypass_expand=True,
                            ),
                            timeout=self.timeout,
                        )
                        await self.send_return_event("returns", result)
                    except Omitted as e:
                        logger.warn(e)
                        await self.send_skip_event("returns")

                    except (asyncio.TimeoutError, Exception) as e:
                        logger.exception(e)
                        await self.send_error_event("returns", e)
                        break

        except asyncio.CancelledError as e:
            await self.log("Atom got cancelled")
            return CancelEvent(diagram_id=self.diagramNode.id, handle="returns")

        except Exception as e:
            logger.exception(e)

    async def assign_handle(self, handle: str, event: EventType):
        await self.inqueue.put(event)
