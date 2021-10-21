from .base import CombinatorAtom
from fluss.diagram import DiagramNode
import asyncio
from typing import Dict
from ....events import *
from ....exceptions import NoHandleException
import logging




logger = logging.getLogger(__name__)

class ZipAtom(CombinatorAtom):
    """The combineLatest operator is one of the combination operators that emits the last value from each of the observable streams when the observable emits a value.
Example, if there are 3 data streams passed as an argument to the combineLatest operator, it will take the latest emitted value by each of the argument streams in that particular order.
You won’t see the result until each of the streams have emitted at least one value. This means if you have passed in 2 argument observables to the operator, the returned observable will always emit an array of 2 values. Now since the resulting array needs to have the same length, the combineLatest operator emits only once each of the streams have emitted at least once.

    Args:
        CombinatorAtom ([type]): [description]
    """


    def __init__(self, diagramNode: DiagramNode, outQueue: asyncio.Queue, constants: Dict = {}, maxsize: int = 0, **kwargs) -> None:
        super().__init__(diagramNode, outQueue, constants=constants, maxsize=maxsize, **kwargs)

        self.arg1_done = False
        self.arg2_done = False

        self.reset()


    def reset(self):

        self.arg1_latest = [None]
        self.arg2_latest = [None]


    async def run(self):
        try:
            while True:

                event1, event2 = await asyncio.gather(self.arg1_queue.get(), self.arg2_queue.get())
                logger.info(f"{event1}, {event2}")

                if isinstance(event1, DoneEvent) or isinstance(event2, DoneEvent):
                    await self.send_done_event("return1")
                    break

                if isinstance(event1, ErrorEvent) or isinstance(event2, ErrorEvent):
                    await self.pass_error_event("return1", event1)
                    break
                
                if (isinstance(event1, YieldEvent) or isinstance(event1, ReturnEvent)) and (isinstance(event2, YieldEvent) or isinstance(event2, ReturnEvent)):
                    await self.send_yield_event("return1", event1.returns +event2.returns)

                


        except asyncio.CancelledError as e:
            await self.log("Atom got cancelled")
            return CancelEvent(diagram_id=self.diagramNode.id, handle="returns")


        except Exception  as e:
            logger.exception(e)



    async def assign_handle(self, handle, event):
        if (handle == "arg1"): 
            logger.error(f"Assignin on handle {event}")
            return await self.arg1_queue.put(event)
        if (handle == "arg2"): return await self.arg2_queue.put(event)
        raise NoHandleException(f"No handle for {handle}")



