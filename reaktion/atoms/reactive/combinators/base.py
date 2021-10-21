from ..base import ReactiveAtom
from fluss.diagram import DiagramNode
import asyncio
from typing import Dict


class CombinatorAtom(ReactiveAtom):

    def __init__(self, diagramNode: DiagramNode, outQueue: asyncio.Queue, constants: Dict = {}, maxsize:int = 0, **kwargs) -> None:
        super().__init__(diagramNode, outQueue, constants=constants, **kwargs)
        self.arg1_queue = asyncio.Queue(maxsize=maxsize)
        self.arg2_queue = asyncio.Queue(maxsize=maxsize)