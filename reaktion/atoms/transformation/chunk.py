import asyncio
from typing import List, Tuple
from reaktion.atoms.combination.base import CombinationAtom
from reaktion.events import EventType, OutEvent, Returns
import logging

logger = logging.getLogger(__name__)


class ChunkAtom(CombinationAtom):
    complete: List[bool] = [False, False]

    async def run(self):
        try:
            while True:
                event = await self.private_queue.get()

                if event.type == EventType.ERROR:
                    await self.event_queue.put(
                        OutEvent(
                            handle="return_0",
                            type=EventType.ERROR,
                            value=event.value,
                            source=self.node.id,
                        )
                    )
                    break

                if event.type == EventType.NEXT:

                    assert (
                        len(event.value) == 1
                    ), "ChunkAtom only supports flattening one value"
                    assert isinstance(
                        event.value[0], list
                    ), "ChunkAtom only supports flattening lists"

                    for value in event.value[0]:
                        await self.event_queue.put(
                            OutEvent(
                                handle="return_0",
                                type=EventType.NEXT,
                                value=[value],
                                source=self.node.id,
                            )
                        )

                if event.type == EventType.COMPLETE:
                    await self.event_queue.put(
                        OutEvent(
                            handle="return_0",
                            type=EventType.COMPLETE,
                            value=[value],
                            source=self.node.id,
                        )
                    )
                    break

        except asyncio.CancelledError as e:
            logger.warning(f"Atom {self.node} is getting cancelled")
            raise e

        except Exception as e:
            logger.exception(f"Atom {self.node} excepted")
