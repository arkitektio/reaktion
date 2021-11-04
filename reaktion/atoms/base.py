from abc import ABC, abstractmethod

from fluss import diagram
from ..events import (
    CancelEvent,
    DoneEvent,
    ErrorEvent,
    EventType,
    ReturnEvent,
    SkipEvent,
    YieldEvent,
)
from typing import AbstractSet, Dict, List
import asyncio
import logging
from arkitekt.messages.postman.log import LogLevel
from fluss.diagram import DiagramNode

logger = logging.getLogger(__name__)


class Atom(ABC):
    def __init__(
        self,
        diagramNode: DiagramNode,
        outQueue: asyncio.Queue,
        constants: Dict = {},
        log=None,
        **kwargs,
    ) -> None:
        self.diagramNode = diagramNode
        self.constants = constants
        self.outQueue = outQueue
        self._log = log
        if self._log:
            assert asyncio.iscoroutinefunction(
                self._log
            ), "Log function must be a coroutine or none"

    async def send_cancelled_event(self, handle):
        await self.outQueue.put(
            CancelEvent(diagram_id=self.diagramNode.id, handle=handle)
        )

    async def send_yield_event(self, handle, yields: List):
        await self.outQueue.put(
            YieldEvent(diagram_id=self.diagramNode.id, handle=handle, returns=yields)
        )

    async def send_return_event(self, handle, yields: List):
        await self.outQueue.put(
            ReturnEvent(diagram_id=self.diagramNode.id, handle=handle, returns=yields)
        )

    async def send_done_event(self, handle):
        await self.outQueue.put(
            DoneEvent(diagram_id=self.diagramNode.id, handle=handle)
        )

    async def send_skip_event(self, handle):
        await self.outQueue.put(
            SkipEvent(
                diagram_id=self.diagramNode.id,
                handle=handle,
                skipper=self.diagramNode.id,
            )
        )

    async def send_error_event(self, handle, e: Exception):
        await self.outQueue.put(
            ErrorEvent(
                diagram_id=self.diagramNode.id,
                handle=handle,
                throwing=self.diagramNode.id,
                exception=str(e.__class__.__name__),
                message=str(e),
            )
        )

    async def pass_error_event(self, handle, event: ErrorEvent):
        await self.outQueue.put(
            ErrorEvent(
                diagram_id=self.diagramNode.id,
                handle=handle,
                exception=event.exception,
                throwing=event.throwing,
                message=event.message,
            )
        )

    async def pass_skip_event(self, handle, event: SkipEvent):
        await self.outQueue.put(
            SkipEvent(
                diagram_id=self.diagramNode.id, handle=handle, skipper=event.skipper
            )
        )

    async def log(self, message: str, logLevel: LogLevel = LogLevel.INFO):
        logger.info(f"{message}")

    @abstractmethod
    async def run(self):
        raise NotImplementedError("Needs to be implemented")

    @abstractmethod
    async def assign_handle(self, handle, event: EventType):
        raise NotImplementedError("Needs to be implemented")
