import asyncio
from arkitekt.postmans.utils import ReservationContract
from reaktion.atoms.generic import MapAtom, MergeMapAtom
from reaktion.events import Returns


class ArkitektMapAtom(MapAtom):
    contract: ReservationContract

    async def map(self, args: Returns) -> Returns:
        await asyncio.sleep(0.1)
        return args
        # return await self.contract.aassign(*args)


class ArkitektMergeMapAtom(MergeMapAtom):
    contract: ReservationContract

    async def merge_map(self, args: Returns) -> Returns:
        async for res in self.contract.aassign(*args):
            yield res
