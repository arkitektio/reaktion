from pydantic import BaseModel, ConfigDict
import asyncio
from reaktion.events import OutEvent



class AtomTransport(BaseModel):
    model_config = ConfigDict( 
        arbitrary_types_allowed=True,
    )
    """ The transport layer for the atom. This is used to send and receive events. """
    queue: asyncio.Queue

    async def put(self, event: OutEvent):
        await self.queue.put(event)

    async def get(self) -> OutEvent:
        return await self.queue.get()



class MockTransport(AtomTransport):
    async def get(self, timeout=3) -> OutEvent:
        return await asyncio.wait_for(self.queue.get(), timeout=timeout)

    pass
