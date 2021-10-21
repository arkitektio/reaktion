from .base import CombinatorAtom
from fluss.diagram import DiagramNode
import asyncio
from typing import Dict
from ....events import *
from ....exceptions import NoHandleException
import logging

logger = logging.getLogger(__name__)

class WithLatestFromAtom(CombinatorAtom):


    def __init__(self, diagramNode: DiagramNode, outQueue: asyncio.Queue, constants: Dict = {}, maxsize: int = 0, **kwargs) -> None:
        super().__init__(diagramNode, outQueue, constants=constants, maxsize=maxsize, **kwargs)
        self.arg1_done = False
        self.arg2_done = False

        self.arg1_latest = [None]
        self.arg2_latest = [None]

    async def run(self):
        try:
            while True:

                if self.arg1_done:
                    await self.log("Arg 1 is done waiting for only Arg 2")
                    event = await self.arg2_queue.get()

                    if isinstance(event, DoneEvent):
                        self.arg2_done = True

                    if isinstance(event, ErrorEvent):
                       await self.pass_error_event("return1", event)
                       break

                    if isinstance(event, YieldEvent) or isinstance(event, ReturnEvent):
                        self.arg2_latest = event.returns
                        if self.arg1_latest != [None]:
                            await self.send_yield_event("return1", self.arg1_latest + self.arg2_latest)

                elif self.arg2_done:
                    await self.log("Arg 2 is done waiting for only Arg 1")
                    event = await self.arg1_queue.get()

                    if isinstance(event, DoneEvent):
                        self.arg1_done = True

                    if isinstance(event, ErrorEvent):
                        await self.pass_error_event("return1", event)
                        break

                    if isinstance(event, YieldEvent) or isinstance(event, ReturnEvent):
                        self.arg1_latest = event.returns
                        if self.arg2_latest != [None]:
                            await self.send_yield_event("return1", self.arg1_latest + self.arg2_latest)

                else:
                    await self.log("Waiting for both Tasks")
                    loop = asyncio.get_event_loop()
                    arg1_task = loop.create_task(self.arg1_queue.get())
                    arg2_task = loop.create_task(self.arg2_queue.get())
                    done, pending = await asyncio.wait(
                            [arg1_task, arg2_task],
                            return_when=asyncio.FIRST_COMPLETED
                    )

                    for x in done:
                        if x == arg1_task: 

                            event = x.result()
                            if isinstance(event, DoneEvent):
                                self.arg1_done = True

                            if isinstance(event, ErrorEvent):
                                await self.pass_error_event("return1", event)

                            if isinstance(event, YieldEvent) or isinstance(event, ReturnEvent):
                                self.arg1_latest = event.returns
                                if self.arg2_latest != [None]:
                                    await self.send_yield_event("return1", self.arg1_latest + self.arg2_latest)


                        if x == arg2_task:
                            event = x.result()

                            if isinstance(event, DoneEvent):
                                self.arg2_done = True

                            if isinstance(event, ErrorEvent):
                                await self.pass_error_event("return1", event)

                            if isinstance(event, YieldEvent) or isinstance(event, ReturnEvent):
                                self.arg2_latest = event.returns
                                if self.arg1_latest != [None]:
                                    await self.send_yield_event("return1", self.arg1_latest + self.arg2_latest)

                        

                    for i in pending:
                        i.cancel()


                    try:
                        await asyncio.gather(*pending)
                    except asyncio.CancelledError:
                        pass

                if self.arg2_done and self.arg1_done:
                    # We are completely done
                    await self.send_done_event("return1")
                    break


        except asyncio.CancelledError as e:
            await self.log("Atom got cancelled")
            return CancelEvent(diagram_id=self.diagramNode.id, handle="returns")


        except Exception  as e:
            logger.exception(e)


    async def assign_handle(self, handle, event):
        if (handle == "arg1"): 
            return await self.arg1_queue.put(event)
        if (handle == "arg2"): return await self.arg2_queue.put(event)
        raise NoHandleException(f"No handle for {handle}")



