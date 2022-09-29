import json
import pytest
from fluss.api.schema import GraphInput, get_flow
import yaml
from .utils import build_relative


@pytest.mark.integration
@pytest.fixture
def app():

    from arkitekt.apps.fluss import FlussApp
    from fakts.discovery.static import StaticDiscovery
    from fakts.fakts import Fakts
    from fakts.grants.remote.claim import ClaimGrant
    from herre.fakts.herre import FaktsHerre

    return FlussApp(
        fakts=Fakts(
            grant=ClaimGrant(
                client_id="DSNwVKbSmvKuIUln36FmpWNVE2KrbS2oRX0ke8PJ",
                client_secret="Gp3VldiWUmHgKkIxZjL2aEjVmNwnSyIGHWbQJo6bWMDoIUlBqvUyoGWUWAe6jI3KRXDOsD13gkYVCZR0po1BLFO9QT4lktKODHDs0GyyJEzmIjkpEOItfdCC4zIa3Qzu",
                discovery=StaticDiscovery(base_url="http://localhost:8000/f/"),
            ),
            force_refresh=True,
        ),
        herre=FaktsHerre(no_temp=True),
    )


@pytest.mark.integration
def test_get_flow(app):

    with app:
        x = get_flow(id=21)
        with open(build_relative("flows/get_flow.yaml"), "w") as f:
            yaml.dump(json.loads(x.graph.json(by_alias=True)), f)


@pytest.mark.integration
def test_create_flow(app):

    with app:

        with open(build_relative("flows/get_flow.yaml"), "r") as f:
            g = yaml.safe_load(f)

        print(g)
        t = GraphInput(**g)
        print(t)
