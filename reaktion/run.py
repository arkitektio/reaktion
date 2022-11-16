import asyncio
from lib2to3.pytree import Base
from typing import Dict, Tuple
from reaktion.agent import ReaktionAgent
from rekuest.agents.errors import ProvisionException

from rekuest.agents.stateful import StatefulAgent
from rekuest.contrib.fakts.websocket_agent_transport import FaktsWebsocketAgentTransport
from rekuest.postmans.utils import use
from pydantic import Field
from rekuest.actors.base import Actor
from rekuest.agents.base import BaseAgent
from rekuest.api.schema import (
    ArgPortInput,
    DefinitionInput,
    NodeFragment,
    NodeKind,
    ReturnPortInput,
    TemplateFragment,
    acreate_template,
    adelete_node,
    afind,
    aget_provision,
    get_template,
)
from rekuest.messages import Assignation, Provision
from rekuest.postmans.utils import ReservationContract
from fakts.fakts import Fakts
from fluss.api.schema import (
    ArgNodeFragment,
    FlowFragment,
    KwargNodeFragment,
    ReturnNodeFragment,
)
from rekuest.widgets import SliderWidget, StringWidget
from arkitekt.apps.fluss import FlussApp
from arkitekt.apps.rekuest import ArkitektRekuest, RekuestApp
from koil.types import Contextual
from reaktion.utils import infer_kind_from_graph
from reaktion.actor import FlowActor
from fakts.grants.remote.claim import ClaimGrant
from fakts.grants.remote.claimprivate import ClaimPrivateGrant


class ConnectedApp(FlussApp, RekuestApp):
    pass


app = ConnectedApp(
    fakts=Fakts(
        subapp="reaktion",
        grant=ClaimPrivateGrant(
            client_id="sdfsdfesegfsefsefef",
            client_secret="Gp3VldiWUmHgKkIxZjL2aEjVmNwnSyIGHWbQJo6bWMDoIUlBqvUyoGWUWAe6jI3KRXDOsD13gkYVCZR0po1BLFO9QT4lktKODHDs0GyyJEzmIjkpEOItfdCC4zIa3Qzu",
        ),
        force_refresh=True,
    ),
    rekuest=ArkitektRekuest(
        agent=ReaktionAgent(
            transport=FaktsWebsocketAgentTransport(fakts_group="rekuest.agent")
        )
    ),
)


@app.rekuest.register(
    widgets={"description": StringWidget(as_paragraph=True)},
    interfaces=["fluss:deploy"],
)
async def deploy_graph(
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

    argNode = [x for x in flow.graph.nodes if isinstance(x, ArgNodeFragment)][0]
    kwargNode = [x for x in flow.graph.nodes if isinstance(x, KwargNodeFragment)][0]
    returnNode = [x for x in flow.graph.nodes if isinstance(x, ReturnNodeFragment)][0]

    args = [ArgPortInput(**x.dict()) for x in flow.graph.args]
    returns = [ReturnPortInput(**x.dict()) for x in flow.graph.returns]

    template = await acreate_template(
        DefinitionInput(
            name=name or flow.workspace.name,
            interface=f"flow-{flow.id}",
            kind=infer_kind_from_graph(flow.graph),
            args=args,
            returns=returns,
            description=description,
            meta={"flow": flow.id},
            interfaces=["workflow", f"diagram:{flow.workspace.id}", f"flow:{flow.id}"],
        ),
        params={"flow": flow.id},
        extensions=["flow"],
    )

    return template


@app.rekuest.register()
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


@app.rekuest.register(widgets={"interval": SliderWidget(min=0, max=100)})
async def timer(interval: int = 4) -> int:
    """Timer

    A simple timer that prints the current time every interval seconds

    Args:
        interval (int, optional): The interval in seconds. Defaults to 4.

    Returns:
        int: The current interval (iteration)

    """
    i = 0
    while True:
        i += 1
        yield i
        await asyncio.sleep(interval)


async def main():
    async with app:
        await app.rekuest.run()


if __name__ == "__main__":

    asyncio.run(main())
