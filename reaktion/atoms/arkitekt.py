import asyncio
from arkitekt.api.schema import AssignationLogLevel
from arkitekt.messages import Assignation
from arkitekt.postmans.utils import ReservationContract
from reaktion.atoms.generic import MapAtom, MergeMapAtom
from reaktion.events import Returns


class ArkitektMapAtom(MapAtom):
    contract: ReservationContract

    async def map(self, args: Returns) -> Returns:
        returns = await self.contract.aassign(*args, alog=self.alog_arkitekt)
        return returns
        # return await self.contract.aassign(*args)

    async def alog_arkitekt(
        self, assignation: Assignation, level: AssignationLogLevel, message: str
    ):
        if self.alog:
            await self.alog(self.node.id, level, message)


class ArkitektMergeMapAtom(MergeMapAtom):
    contract: ReservationContract

    async def merge_map(self, args: Returns) -> Returns:
        async for res in self.contract.aassign(*args):
            yield res
