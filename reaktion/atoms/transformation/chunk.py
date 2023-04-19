import asyncio
from typing import List
from reaktion.atoms.combination.base import CombinationAtom
from reaktion.events import EventType, OutEvent
import logging

logger = logging.getLogger(__name__)


class ChunkAtom(CombinationAtom):
    complete: List[bool] = [False, False]

    async def run(self):
        try:
            while True:
                event = await self.get()

                if event.type == EventType.ERROR:
                    await self.transport.put(
                        OutEvent(
                            handle="return_0",
                            type=EventType.ERROR,
                            value=event.value,
                            source=self.node.id,
                            caused_by=[event.current_t],
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

                    if self.node.defaults:
                        iterations = self.node.defaults.get("iterations", 1)
                    else:
                        iterations = 1

                    for i in range(iterations):
                        for value in event.value[0]:
                            await self.transport.put(
                                OutEvent(
                                    handle="return_0",
                                    type=EventType.NEXT,
                                    value=[value],
                                    source=self.node.id,
                                    caused_by=[event.current_t],
                                )
                            )

                            if self.node.defaults:
                                sleep = self.node.defaults.get("sleep", None)
                                print("Sleeping in interval", sleep)
                                if sleep:
                                    await asyncio.sleep(sleep * 0.001)

                        if self.node.defaults:
                            sleep = self.node.defaults.get("iterations_sleep", None)
                            if sleep:
                                await asyncio.sleep(sleep * 0.001)

                if event.type == EventType.COMPLETE:
                    await self.transport.put(
                        OutEvent(
                            handle="return_0",
                            type=EventType.COMPLETE,
                            value=[],
                            source=self.node.id,
                            caused_by=[event.current_t],
                        )
                    )
                    break

        except asyncio.CancelledError as e:
            logger.warning(f"Atom {self.node} is getting cancelled")
            raise e

        except Exception as e:
            logger.exception(f"Atom {self.node} excepted")
            raise e
