import asyncio
from reaktion.agent import ReaktionAgent

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
from rekuest.widgets import SliderWidget, StringWidget
from arkitekt.apps.fluss import FlussApp
from arkitekt.apps.unlok import UnlokApp
from arkitekt.apps.rekuest import ArkitektRekuest, RekuestApp
from reaktion.utils import infer_kind_from_graph
from fakts.grants.remote.device_code import DeviceCodeGrant
from fakts.fakts import Fakts
from fakts.grants import CacheGrant
from herre.grants import CacheGrant as HerreCacheGrant
from herre.grants.oauth2.refresh import RefreshGrant
from herre.grants.fakts import FaktsGrant
from fakts.grants.remote import Manifest
from fakts.discovery import StaticDiscovery
from herre import Herre


class ConnectedApp(FlussApp, RekuestApp, UnlokApp):
    pass


identifier = "fluss"
version = "latest"
url = "http://localhost:8000/f/"

app = ConnectedApp(
    fakts=Fakts(
        grant=CacheGrant(
            cache_file=f"{identifier}-{version}_cache.json",
            hash=f"{identifier}-{version}-{url}",
            grant=DeviceCodeGrant(
                manifest=Manifest(
                    identifier=identifier,
                    version=version,
                    scopes=["read", "write"],
                ),
                discovery=StaticDiscovery(base_url=url),
            ),
        )
    ),
    herre=Herre(
        grant=HerreCacheGrant(
            cache_file=f"{identifier}-{version}_herre_cache.json",
            hash=f"{identifier}-{version}-{url}",
            grant=RefreshGrant(grant=FaktsGrant()),
        ),
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

    print([x.dict(by_alias=True) for x in flow.graph.args])
    print([x.dict(by_alias=True) for x in flow.graph.returns])

    # assert localnodes are in the definitionregistry
    localNodes = [x for x in flow.graph.nodes if isinstance(x, LocalNodeFragment)]
    for node in localNodes:
        assert node.hash, f"LocalNode {node.name} must have a definition"
        assert (
            node.hash in app.rekuest.agent.nodeHashActorMap
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
            interfaces=["workflow", f"diagram:{flow.workspace.id}", f"flow:{flow.id}"],
        ),
        instance_id=app.rekuest.agent.instance_id,
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
