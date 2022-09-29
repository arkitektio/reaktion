from asyncio.tasks import create_task
from reaktion.actor import FlowActor
from rekuest.agents.stateful import StatefulAgent
import logging
from rekuest.actors.base import Actor

from rekuest.api.schema import aget_provision
from rekuest.messages import Provision

logger = logging.getLogger(__name__)


class ReaktionAgent(StatefulAgent):
    async def aspawn_actor(self, message: Provision) -> Actor:
        """Spawns an Actor from a Provision"""
        prov = await aget_provision(message.provision)

        if prov.template.id in self._templateActorBuilderMap:
            actor_builder = self._templateActorBuilderMap[prov.template.id]
        else:
            actor_builder = FlowActor

        actor = actor_builder(provision=prov, transport=self.transport)
        await actor.arun()
        self.provisionActorMap[prov.id] = actor
        self.provisionProvisionMap[prov.id] = prov
        return actor
