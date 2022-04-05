import asyncio
from pydantic import BaseModel
from fluss.api.schema import FlowNodeFragment
from reaktion.atoms.errors import AtomQueueFull
from reaktion.events import InEvent, OutEvent
import logging

logger = logging.getLogger(__name__)


class Atom(BaseModel):
    node: FlowNodeFragment
    private_queue: asyncio.Queue[InEvent]
    event_queue: asyncio.Queue[OutEvent]

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
        json_encoders = {
            asyncio.Queue: lambda q: repr(q),
        }
