import asyncio
from typing import List, Optional
from reaktion.atoms.helpers import index_for_handle
from reaktion.atoms.combination.base import CombinationAtom
from reaktion.events import EventType, OutEvent, InEvent
import logging
import functools
from typing import Optional

logger = logging.getLogger(__name__)


class WithLatestAtom(CombinationAtom):
    state: List[Optional[InEvent]] = [None, None]
    complete: List[bool] = [None, None]

    async def run(self):
        self.state = list(map(lambda x: None, self.node.instream))
        self.complete = list(map(lambda x: False, self.node.instream))

        try:
            while True:
                event = await self.get()
                print("WithLatestAtom", event)

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

                streamIndex = index_for_handle(event.handle)
                print(streamIndex)

                if event.type == EventType.COMPLETE:
                    self.complete[streamIndex] = True

                    if streamIndex == 0:
                        await self.transport.put(
                            OutEvent(
                                handle="return_0",
                                type=EventType.COMPLETE,
                                source=self.node.id,
                                caused_by=[event.current_t],
                            )
                        )
                        break

                if event.type == EventType.NEXT:
                    self.state[streamIndex] = event

                    if self.state[0] is not None and self.state[1] is not None:
                        await self.transport.put(
                            OutEvent(
                                handle="return_0",
                                type=EventType.NEXT,
                                value=functools.reduce(
                                    lambda a, b: a.value + b.value, self.state
                                ),
                                source=self.node.id,
                                caused_by=[
                                    self.state[0].current_t,
                                    self.state[1].current_t,
                                ],
                            )
                        )

        except asyncio.CancelledError as e:
            logger.warning(f"Atom {self.node} is getting cancelled")
            raise e

        except Exception:
            logger.exception(f"Atom {self.node} excepted")
