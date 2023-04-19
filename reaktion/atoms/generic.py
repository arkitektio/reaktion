import asyncio
from reaktion.events import OutEvent, Returns, EventType
from reaktion.atoms.base import Atom
import logging

logger = logging.getLogger(__name__)


class MapAtom(Atom):
    async def map(self, value: Returns) -> Returns:
        raise NotImplementedError("This needs to be implemented")

    async def run(self):
        try:
            while True:
                event = await self.get()

                if event.type == EventType.NEXT:
                    try:
                        result = await self.map(event.value)
                        if result is None:
                            value = ()
                        elif isinstance(result, list) or isinstance(result, tuple):
                            value = result
                        else:
                            value = (result,)

                        await self.transport.put(
                            OutEvent(
                                handle="return_0",
                                type=EventType.NEXT,
                                value=value,
                                source=self.node.id,
                                caused_by=[event.current_t],
                            )
                        )
                    except Exception as e:
                        logger.error(f"{self.node.id} map failed")
                        await self.transport.put(
                            OutEvent(
                                handle="return_0",
                                type=EventType.ERROR,
                                source=self.node.id,
                                value=e,
                                caused_by=[event.current_t],
                            )
                        )
                        break

                if event.type == EventType.COMPLETE:
                    # Everything left of us is done, so we can shut down as well
                    await self.transport.put(
                        OutEvent(
                            handle="return_0",
                            type=EventType.COMPLETE,
                            source=self.node.id,
                            caused_by=[event.current_t],
                        )
                    )
                    break  # Everything left of us is done, so we can shut down as well

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
                event = await self.get()

                if event.type == EventType.NEXT:
                    try:
                        async for result in self.merge_map(event.value):
                            if result is None:
                                value = ()
                            elif isinstance(result, list) or isinstance(result, tuple):
                                value = result
                            else:
                                value = (result,)
                            await self.transport.put(
                                OutEvent(
                                    handle="return_0",
                                    type=EventType.NEXT,
                                    value=value,
                                    source=self.node.id,
                                    caused_by=[event.current_t],
                                )
                            )
                    except Exception as e:
                        logger.error(f"{self.node.id} map failed")
                        await self.transport.put(
                            OutEvent(
                                handle="return_0",
                                type=EventType.ERROR,
                                source=self.node.id,
                                value=e,
                                caused_by=[event.current_t],
                            )
                        )
                        break

                if event.type == EventType.COMPLETE:
                    # Everything left of us is done, so we can shut down as well
                    await self.transport.put(
                        OutEvent(
                            handle="return_0",
                            type=EventType.COMPLETE,
                            source=self.node.id,
                            caused_by=[event.current_t],
                        )
                    )
                    break  # Everything left of us is done, so we can shut down as well

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
                    # We are not raising the exception here but monadicly killing it to the
                    # left
        except asyncio.CancelledError as e:
            logger.warning(f"Atom {self.node} is getting cancelled")
            raise e
