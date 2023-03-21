from asyncio.tasks import create_task
from reaktion.actor import FlowActor
from rekuest.agents.errors import ProvisionException
from rekuest.agents.stateful import StatefulAgent
import logging
from rekuest.actors.base import Actor
from fluss.api.schema import aget_flow
from rekuest.api.schema import aget_template, NodeKind
from rekuest.messages import Provision
from reaktion.errors import ReaktionError

logger = logging.getLogger(__name__)


class ReaktionAgent(StatefulAgent):
    async def aspawn_actor(self, prov: Provision) -> Actor:
        """Spawns an Actor from a Provision"""
        try:
            actor_builder = self._templateActorBuilderMap[prov.template]
            actor = actor_builder(provision=prov, transport=self.transport)
        except KeyError as e:
            try:
                x = await aget_template(prov.template)
                assert "flow" in x.params, "Template is not a flow"

                t = await aget_flow(id=x.params["flow"])
                actor = FlowActor(
                    provision=prov,
                    transport=self.transport,
                    flow=t,
                    is_generator=x.node.kind == NodeKind.GENERATOR,
                )

            except Exception as e:
                raise ProvisionException("No Actor Builders found for template") from e

        task = await actor.arun()
        self.provisionActorMap[prov.provision] = actor
        return actor
