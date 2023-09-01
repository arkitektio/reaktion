from reaktion.actor import FlowActor
from rekuest.agents.errors import ProvisionException
from rekuest.agents.base import BaseAgent
from rekuest.actors.reactive.api import useInstanceID
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
from typing import Optional
from rekuest.api.schema import (
    PortInput,
    DefinitionInput,
    TemplateFragment,
    NodeKind,
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
from rekuest.agents.extension import AgentExtension

logger = logging.getLogger(__name__)
from rekuest.definition.registry import DefinitionRegistry
from rekuest.actors.actify import reactify
from rekuest.actors.base import ActorTransport


class ReaktionExtension(BaseModel):
    structure_registry: StructureRegistry = Field(
        default_factory=get_default_structure_registry
    )

    async def aspawn_actor_from_template(
        self,
        template: TemplateFragment,
        passport: Passport,
        transport: ActorTransport,
        agent: "BaseAgent",
    ) -> Optional[Actor]:
        """Spawns an Actor from a Provision. This function closely mimics the
        spawining protocol within an actor. But maps template"""

        x = template
        assert "flow" in x.params, "Template is not a flow"

        t = await aget_flow(id=x.params["flow"])

        return FlowActor(
            flow=t,
            is_generator=x.node.kind == NodeKind.GENERATOR,
            passport=passport,
            transport=transport,
            definition=x.node,
            agent=agent,
            collector=agent.collector,
        )

    async def aregister_definitions(
        self, definition_registry: DefinitionRegistry, instance_id: str = None
    ):
        definition, actorBuilder = reactify(
            self.deploy_graph,
            self.structure_registry,
            widgets={"description": StringWidget(as_paragraph=True)},
            interfaces=["fluss:deploy"],
        )

        definition_registry.register_at_interface(
            "deploy_graph",
            definition=definition,
            structure_registry=self.structure_registry,
            actorBuilder=actorBuilder,
        )

        definition, actorBuilder = reactify(
            self.undeploy_graph,
            self.structure_registry,
            interfaces=["fluss:undeploy"],
        )

        definition_registry.register_at_interface(
            "undeploy_graph",
            definition=definition,
            structure_registry=self.structure_registry,
            actorBuilder=actorBuilder,
        )

    async def deploy_graph(
        self,
        flow: FlowFragment,
        name: str = None,
        description: str = None,
        kind: Optional[NodeKind] = None,
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
                flow, name=name, description=description, kind=kind
            ),
            instance_id=useInstanceID(),
            params={"flow": flow.id},
            extensions=["reaktion"],
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
