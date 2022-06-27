import asyncio
from typing import Dict
from arkitekt.postmans.utils import use
from pydantic import Field
from arkitekt.actors.base import Actor
from arkitekt.agents.base import BaseAgent
from arkitekt.api.schema import (
    ArgPortInput,
    DefinitionInput,
    KwargPortInput,
    NodeTypeInput,
    ReturnPortInput,
    TemplateFragment,
    acreate_template,
    adefine,
    afind,
)
from arkitekt.messages import Assignation, Provision
from arkitekt.postmans.utils import ReservationContract
from fakts.fakts import Fakts
from fluss.api.schema import (
    ArgNodeFragment,
    ArkitektNodeFragment,
    FlowFragment,
    FlowNodeFragment,
    KwargNodeFragment,
    ReturnNodeFragment,
    aget_flow,
    flow,
)
from fluss.arkitekt import ConnectedApp
from koil.types import Contextual


app = ConnectedApp(fakts=Fakts(subapp="reaktion"))


@app.arkitekt.register()
async def deploy_graph(
    flow: FlowFragment,
) -> TemplateFragment:
    """Deploy Flow

    Deploys a Flow as a Template

    Args:
        graph (FlowFragment): The Flow

    Returns:
        TemplateFragment: The Template
    """
    assert flow.name, "Graph must have a Name in order to be deployed"

    argNode = [x for x in flow.nodes if isinstance(x, ArgNodeFragment)][0]
    kwargNode = [x for x in flow.nodes if isinstance(x, KwargNodeFragment)][0]
    returnNode = [x for x in flow.nodes if isinstance(x, ReturnNodeFragment)][0]

    args = [ArgPortInput(**x.dict()) for x in argNode.args]
    kwargs = [KwargPortInput(**x.dict()) for x in kwargNode.kwargs]
    returns = [ReturnPortInput(**x.dict()) for x in returnNode.returns]

    node = await adefine(
        DefinitionInput(
            name=flow.name,
            interface=flow.name,
            type=NodeTypeInput.FUNCTION,
            args=args,
            kwargs=kwargs,
            returns=returns,
        )
    )

    return await acreate_template(node, params={"flow": flow.id})


@app.arkitekt.agent.hook("before_spawn")
async def before_spawn(self: BaseAgent, provision: Provision):
    if provision.template in self._templateActorBuilderMap:
        return
    else:
        print("Halloionioni")
        self._templateActorBuilderMap[provision.template] = FlowActor


class FlowActor(Actor):
    contracts: Dict[str, ReservationContract] = Field(default_factory=dict)
    flow: Contextual[FlowFragment]

    async def on_provide(self, provision: Provision, template: TemplateFragment):

        self.flow = await aget_flow(id=template.params["flow"])

        argNode = [x for x in self.flow.nodes if isinstance(x, ArgNodeFragment)][0]
        kwargNode = [x for x in self.flow.nodes if isinstance(x, KwargNodeFragment)][0]
        returnNode = [x for x in self.flow.nodes if isinstance(x, ReturnNodeFragment)][
            0
        ]

        arkitektNodes = [
            x for x in self.flow.nodes if isinstance(x, ArkitektNodeFragment)
        ]

        instances = {
            x.id: await afind(package=x.package, interface=x.interface)
            for x in arkitektNodes
        }

        self.contracts = {key: use(value) for key, value in instances.items()}

        for contract in self.contracts.values():
            await contract.connect()

    async def on_assign(self, assignation: Assignation):
        print(assignation)

    async def on_unprovide(self):

        for contract in self.contracts.values():
            await contract.adisconnect()


async def main():
    async with app:
        await app.arkitekt.run()


if __name__ == "__main__":

    asyncio.run(main())
