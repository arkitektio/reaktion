import asyncio

print(asyncio.Queue)
from typing import Any, List, Optional
from rekuest.postmans.utils import RPCContract

from fluss.api.schema import ArkitektNodeFragment

from reaktion.atoms.generic import MapAtom, MergeMapAtom, AsCompletedAtom, OrderedAtom
from reaktion.events import Returns
import logging

logger = logging.getLogger(__name__)


class ArkitektMapAtom(MapAtom):
    node: ArkitektNodeFragment
    contract: RPCContract

    async def map(self, args: Returns) -> Optional[List[Any]]:
        defaults = self.node.defaults or {}

        stream_one = self.node.instream[0]
        for arg, item in zip(args, stream_one):
            defaults[item.key] = arg

        returns = await self.contract.aassign(
            args=[], kwargs=defaults, parent=self.assignment
        )
        return returns
        # return await self.contract.aassign(*args)


class ArkitektMergeMapAtom(MergeMapAtom):
    node: ArkitektNodeFragment
    contract: RPCContract

    async def merge_map(self, args: Returns) -> Optional[List[Any]]:
        defaults = self.node.defaults or {}

        stream_one = self.node.instream[0]
        for arg, item in zip(args, stream_one):
            defaults[item.key] = arg

        async for r in self.contract.astream(
            args=[], kwargs=defaults, parent=self.assignment
        ):
            yield r


class ArkitektAsCompletedAtom(AsCompletedAtom):
    node: ArkitektNodeFragment
    contract: RPCContract

    async def map(self, args: Returns) -> Optional[List[Any]]:
        defaults = self.node.defaults or {}

        stream_one = self.node.instream[0]
        for arg, item in zip(args, stream_one):
            defaults[item.key] = arg

        returns = await self.contract.aassign(
            args=[], kwargs=defaults, parent=self.assignment
        )
        return returns


class ArkitektOrderedAtom(OrderedAtom):
    node: ArkitektNodeFragment
    contract: RPCContract

    async def map(self, args: Returns) -> Optional[List[Any]]:
        defaults = self.node.defaults or {}

        stream_one = self.node.instream[0]
        for arg, item in zip(args, stream_one):
            defaults[item.key] = arg

        returns = await self.contract.aassign(
            args=[], kwargs=defaults, parent=self.assignment
        )
        return returns
