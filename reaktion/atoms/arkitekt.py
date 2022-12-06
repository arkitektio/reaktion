import asyncio
from typing import Any, List, Optional
from rekuest.api.schema import AssignationLogLevel
from rekuest.messages import Assignation
from rekuest.postmans.utils import ReservationContract, RPCContract
from fluss.api.schema import ArkitektNodeFragment
from reaktion.atoms.generic import MapAtom, MergeMapAtom
from reaktion.events import Returns
import logging

logger = logging.getLogger(__name__)


class ArkitektMapAtom(MapAtom):
    node: ArkitektNodeFragment
    contract: RPCContract

    async def map(self, args: Returns) -> Optional[List[Any]]:
        defaults = self.node.defaults or {}
        returns = await self.contract.aassign(
            *args, **defaults, parent=self.assignation.assignation
        )
        return returns
        # return await self.contract.aassign(*args)


class ArkitektMergeMapAtom(MergeMapAtom):
    node: ArkitektNodeFragment
    contract: RPCContract

    async def merge_map(self, args: Returns) -> Optional[List[Any]]:
        defaults = self.node.defaults or {}

        async for r in self.contract.astream(
            *args, **defaults, parent=self.assignation.assignation
        ):
            yield r
