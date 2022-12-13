import json
import pytest
from fluss.api.schema import (
    FlowFragment,
)
from .utils import build_relative


def build_flow(path):
    with open(build_relative(path), "r") as f:
        g = json.load(f)
        print(g)

    return FlowFragment(**g)


@pytest.fixture
def add_three_flow():
    return build_flow("flowjsons/add_three_flow.json")


@pytest.fixture
def retrieve_chunk_flow():
    return build_flow("flowjsons/retrieve_chunk_flow.json")
