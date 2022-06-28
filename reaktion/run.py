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
from reaktion.utils import infer_type_from_graph
from reaktion.actor import FlowFuncActor
from fakts.grants.remote.claim import ClaimGrant

app = ConnectedApp(
    fakts=Fakts(
        subapp="reaktion",
        grant=ClaimGrant(
            client_id="DSNwVKbSmvKuIUln36FmpWNVE2dfrsd2oRX0ke8PJ",
            client_secret="Gp3VldiWUmHgKkIxZjL2eoinoeinNwnSyIGHWbQJo6bWMDoIUlBqvUyoGWUWAe6jI3KRXDOsD13gkYVCZR0po1BLFO9QT4lktKODHDs0GyyJEzmIjkpEOItfdCC4zIa3Qzu",
            scopes=["read", "write"],
        ),
    )
)


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

    argNode = [x for x in flow.graph.nodes if isinstance(x, ArgNodeFragment)][0]
    kwargNode = [x for x in flow.graph.nodes if isinstance(x, KwargNodeFragment)][0]
    returnNode = [x for x in flow.graph.nodes if isinstance(x, ReturnNodeFragment)][0]

    args = [ArgPortInput(**x.dict()) for x in argNode.instream[0]]
    kwargs = []
    returns = [ReturnPortInput(**x.dict()) for x in returnNode.outstream[0]]

    node = await adefine(
        DefinitionInput(
            name=flow.name,
            interface=flow.name,
            type=infer_type_from_graph(flow.graph),
            args=args,
            kwargs=kwargs,
            returns=returns,
        )
    )

    return await acreate_template(node, params={"flow": flow.id}, extensions=["flow"])


@app.arkitekt.agent.hook("before_spawn")
async def before_spawn(self: BaseAgent, provision: Provision):
    if provision.template in self._templateActorBuilderMap:
        return
    else:
        self._templateActorBuilderMap[provision.template] = FlowFuncActor


async def main():
    async with app:
        await app.arkitekt.run()


if __name__ == "__main__":

    asyncio.run(main())
