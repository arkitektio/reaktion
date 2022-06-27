import asyncio
from reaktion.events import InEvent, OutEvent, Returns, EventType
from reaktion.atoms.base import Atom
import logging

logger = logging.getLogger(__name__)


class MapAtom(Atom):
    async def map(self, value: Returns) -> Returns:
        raise NotImplementedError("This needs to be implemented")

    async def run(self):
        try:
            while True:
                event = await self.private_queue.get()
                if event.type == EventType.NEXT:
                    try:
                        result = await self.map(event.value)
                        print("Result:", result)
                        await self.event_queue.put(
                            OutEvent(
                                handle="return_0",
                                type=EventType.NEXT,
                                value=result,
                                source=self.node.id,
                            )
                        )
                    except Exception as e:
                        logger.error(f"{self.node.id} map failed")
                        await self.event_queue.put(
                            OutEvent(
                                handle="return_0",
                                type=EventType.ERROR,
                                source=self.node.id,
                                value=e,
                            )
                        )

                if event.type == EventType.COMPLETE:
                    # Everything left of us is done, so we can shut down as well
                    await self.event_queue.put(
                        OutEvent(
                            handle="return_0",
                            type=EventType.COMPLETE,
                            source=self.node.id,
                        )
                    )
                    break  # Everything left of us is done, so we can shut down as well

                if event.type == EventType.ERROR:
                    await self.event_queue.put(
                        OutEvent(
                            handle="return_0",
                            type=EventType.ERROR,
                            value=event.value,
                            source=self.node.id,
                        )
                    )
                    # We are not raising the exception here but monadicly killing it to the
                    # left
        except asyncio.CancelledError as e:
            logger.warning(f"Atom {self.node} is getting cancelled")
            raise e


class MergeMapAtom(Atom):
    async def merge_map(self, value: Returns) -> Returns:
        raise NotImplementedError("This needs to be implemented")

    async def run(self):
        try:
            while True:
                event = await self.private_queue.get()
                if event.type == EventType.NEXT:

                    async for returns in self.merge_map(event.value):
                        await self.event_queue.put(
                            OutEvent(
                                handle="return_0",
                                type=EventType.NEXT,
                                value=returns,
                                source=self.node.id,
                            )
                        )

                if event.type == EventType.COMPLETE:
                    # Everything left of us is done, so we can shut down as well
                    await self.event_queue.put(
                        OutEvent(
                            handle="return_0",
                            type=EventType.COMPLETE,
                            source=self.node.id,
                        )
                    )
                    break  # Everything left of us is done, so we can shut down as well

                if event.type == EventType.ERROR:
                    await self.event_queue.put(
                        OutEvent(
                            handle="return_0",
                            type=EventType.ERROR,
                            value=event.value,
                            source=self.node.id,
                        )
                    )
                    # We are not raising the exception here but monadicly killing it to the
                    # left
        except asyncio.CancelledError as e:
            logger.warning(f"Atom {self.node} is getting cancelled")
            raise e
