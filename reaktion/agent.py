from reaktion.actor import FlowActor
from rekuest.agents.errors import ProvisionException
from rekuest.agents.stateful import StatefulAgent
import logging
from rekuest.actors.base import Actor
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
)
from reaktion.utils import infer_kind_from_graph
from rekuest.widgets import SliderWidget, StringWidget

logger = logging.getLogger(__name__)


class ReaktionAgent(StatefulAgent):
    async def aspawn_actor(self, prov: Provision) -> Actor:
        """Spawns an Actor from a Provision"""
        try:
            actor_builder = self._templateActorBuilderMap[prov.template]
            actor = actor_builder(provision=prov, transport=self.transport)
        except KeyError:
            try:
                x = await aget_template(prov.template)
                assert "flow" in x.params, "Template is not a flow"

                t = await aget_flow(id=x.params["flow"])
                actor = FlowActor(
                    provision=prov,
                    transport=self.transport,
                    agent=self,
                    flow=t,
                    is_generator=x.node.kind == NodeKind.GENERATOR,
                )

            except Exception as e:
                raise ProvisionException("No Actor Builders found for template") from e

        await actor.arun()
        self.provisionActorMap[prov.provision] = actor
        return actor

    async def aregister_definitions(self):
        self.definition_registry.register(
            self.deploy_graph,
            structure_registry=self.definition_registry.structure_registry,
            widgets={"description": StringWidget(as_paragraph=True)},
            interfaces=["fluss:deploy"],
        )
        self.definition_registry.register(
            self.undeploy_graph,
            structure_registry=self.definition_registry.structure_registry,
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

        print([x.dict(by_alias=True) for x in flow.graph.args])
        print([x.dict(by_alias=True) for x in flow.graph.returns])

        # assert localnodes are in the definitionregistry
        localNodes = [x for x in flow.graph.nodes if isinstance(x, LocalNodeFragment)]
        for node in localNodes:
            assert node.hash, f"LocalNode {node.name} must have a definition"
            assert (
                node.hash in self.nodeHashActorMap
            ), f"LocalNode {node.name} is not registered with the agent of this instance"

        args = [PortInput(**x.dict(by_alias=True)) for x in flow.graph.args]
        returns = [PortInput(**x.dict(by_alias=True)) for x in flow.graph.returns]

        template = await acreate_template(
            DefinitionInput(
                name=name or flow.workspace.name,
                interface=f"flow-{flow.id}",
                kind=infer_kind_from_graph(flow.graph),
                args=args,
                returns=returns,
                description=description,
                interfaces=[
                    "workflow",
                    f"diagram:{flow.workspace.id}",
                    f"flow:{flow.id}",
                ],
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
