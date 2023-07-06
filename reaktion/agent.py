from reaktion.actor import FlowActor
from rekuest.agents.errors import ProvisionException
from rekuest.agents.base import BaseAgent

import logging
from rekuest.register import register_func
from rekuest.actors.base import Actor
from rekuest.actors.types import Passport, Assignment
from rekuest.actors.transport.local_transport import (
    AgentActorTransport,
    AgentActorAssignTransport,
)
from fluss.api.schema import aget_flow
from rekuest.api.schema import aget_template, NodeKind
from rekuest.messages import Provision

from rekuest.contrib.fakts.websocket_agent_transport import FaktsWebsocketAgentTransport
from rekuest.api.schema import (
    PortInput,
    DefinitionInput,
    TemplateFragment,
    acreate_template,
    adelete_node,
    afind,
)
from fakts.fakts import Fakts
from fluss.api.schema import (
    FlowFragment,
    LocalNodeFragment,
    GraphNodeFragment,
)
from reaktion.utils import infer_kind_from_graph
from rekuest.widgets import SliderWidget, StringWidget
from rekuest.structures.default import get_default_structure_registry
from rekuest.structures.registry import StructureRegistry
from pydantic import BaseModel, Field
from .utils import convert_flow_to_definition

logger = logging.getLogger(__name__)


class ReaktionAgent(BaseAgent):
    structure_registry: StructureRegistry = Field(
        default_factory=get_default_structure_registry
    )

    async def aspawn_actor_from_provision(self, provision: Provision) -> Actor:
        """Spawns an Actor from a Provision. This function closely mimics the
        spawining protocol within an actor. But maps template"""

        try:
            interface = self.template_interface_map[provision.template]

            actor_builder = self.definition_registry.get_builder_for_interface(
                interface
            )

            passport = Passport(provision=provision.provision)

            actor = actor_builder(
                passport=passport,
                transport=AgentActorTransport(
                    passport=passport, agent_transport=self.transport
                ),
                definition_registry=self.definition_registry,
                collector=self.collector,
            )

        except KeyError as e:
            x = await aget_template(provision.template)
            assert "flow" in x.params, "Template is not a flow"

            t = await aget_flow(id=x.params["flow"])

            passport = Passport(provision=provision.provision)
            actor = FlowActor(
                flow=t,
                is_generator=x.node.kind == NodeKind.GENERATOR,
                passport=passport,
                transport=AgentActorTransport(
                    passport=passport, agent_transport=self.transport
                ),
                definition=x.node,
                definition_registry=self.definition_registry,
                collector=self.collector,
            )

        await actor.arun()  # TODO: Maybe move this outside?
        self.managed_actors[passport.id] = actor
        self.provision_passport_map[provision.provision] = passport
        return actor

    async def aregister_definitions(self):
        register_func(
            self.deploy_graph,
            structure_registry=self.structure_registry,
            definition_registry=self.definition_registry,
            widgets={"description": StringWidget(as_paragraph=True)},
            interfaces=["fluss:deploy"],
        )
        register_func(
            self.undeploy_graph,
            structure_registry=self.structure_registry,
            definition_registry=self.definition_registry,
            interfaces=["fluss:undeploy"],
        )

        return await super().aregister_definitions()

    async def deploy_graph(
        self,
        flow: FlowFragment,
        name: str = None,
        description: str = None,
    ) -> TemplateFragment:
        """Deploy Flow

        Deploys a Flow as a Template

        Args:
            graph (FlowFragment): The Flow
            name (str, optional): The name of this Incarnation
            description (str, optional): The name of this Incarnation

        Returns:
            TemplateFragment: The created template
        """
        assert flow.name, "Graph must have a Name in order to be deployed"

        template = await acreate_template(
            interface=f"flow:{flow.id}",
            definition=convert_flow_to_definition(
                flow, name=name, description=description
            ),
            instance_id=self.instance_id,
            params={"flow": flow.id},
            extensions=["flow"],
        )

        return template

    async def undeploy_graph(
        flow: FlowFragment,
    ):
        """Undeploy Flow

        Undeploys graph, no user will be able to reserve this graph anymore

        Args:
            graph (FlowFragment): The Flow

        """
        assert flow.name, "Graph must have a Name in order to be deployed"

        x = await afind(interface=flow.hash)

        await adelete_node(x)
        return None
