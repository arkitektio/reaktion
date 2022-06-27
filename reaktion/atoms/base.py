import asyncio
from typing import Awaitable, Callable
from pydantic import BaseModel, Field
from tomlkit import string
from arkitekt.api.schema import AssignationLogLevel
from arkitekt.messages import Assignation
from fluss.api.schema import FlowNodeCommonsFragmentBase
from rath.scalars import ID
from reaktion.atoms.errors import AtomQueueFull
from reaktion.events import InEvent, OutEvent
import logging

logger = logging.getLogger(__name__)


class Atom(BaseModel):
    node: FlowNodeCommonsFragmentBase
    private_queue: asyncio.Queue[InEvent]
    event_queue: asyncio.Queue[OutEvent]
    alog: Callable[[str, AssignationLogLevel, str], Awaitable[None]] = Field(
        exclude=True
    )

    async def run(self):
        raise NotImplementedError("This needs to be implemented")

    async def put(self, event: InEvent):
        try:
            await self.private_queue.put(event)  # TODO: Make put no wait?
        except asyncio.QueueFull as e:
            logger.error(f"{self.node.id} private queue is full")
            raise AtomQueueFull(f"{self.node.id} private queue is full") from e

    class Config:
        arbitrary_types_allowed = True
        underscore_attrs_are_private = True
