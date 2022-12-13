import json
import pytest
from fluss.api.schema import (
    FlowNodeFragmentBaseArkitektNode,
    FlowNodeFragmentBasePosition,
    FlowNodeFragmentBaseReactiveNode,
    StreamItemFragment,
    ReactiveImplementationModelInput,
    StreamKind,
    MapStrategy,
)
from rekuest.api.schema import NodeKind


@pytest.fixture
def arkitekt_generator_node():
    return FlowNodeFragmentBaseArkitektNode(
        id=1,
        position=FlowNodeFragmentBasePosition(x=0, y=0),
        name="add_generator",
        hash="oisnosinsoin",
        kind=NodeKind.GENERATOR,
        mapStrategy=MapStrategy.MAP,
        reserveParams={},
        allowLocal=False,
        defaults={},
        constream=[],
        instream=[[StreamItemFragment(key=1, kind=StreamKind.INT, nullable=False)]],
        outstream=[[StreamItemFragment(key=1, kind=StreamKind.INT, nullable=False)]],
    )


@pytest.fixture
def arkitekt_functional_node():
    return FlowNodeFragmentBaseArkitektNode(
        id=1,
        position=FlowNodeFragmentBasePosition(x=0, y=0),
        name="add_function",
        hash="oisnosinsoin",
        kind=NodeKind.FUNCTION,
        mapStrategy=MapStrategy.MAP,
        reserveParams={},
        allowLocal=False,
        defaults={},
        constream=[],
        instream=[[StreamItemFragment(key=1, kind=StreamKind.INT, nullable=False)]],
        outstream=[[StreamItemFragment(key=1, kind=StreamKind.INT, nullable=False)]],
    )


@pytest.fixture
def reactive_zip_node():
    return FlowNodeFragmentBaseReactiveNode(
        id=1,
        position=FlowNodeFragmentBasePosition(x=0, y=0),
        implementation=ReactiveImplementationModelInput.ZIP,
        defaults={},
        constream=[],
        instream=[[StreamItemFragment(key=1, kind=StreamKind.INT, nullable=False)]],
        outstream=[[StreamItemFragment(key=1, kind=StreamKind.INT, nullable=False)]],
    )


@pytest.fixture
def reactive_withlatest_node():
    return FlowNodeFragmentBaseReactiveNode(
        id=1,
        position=FlowNodeFragmentBasePosition(x=0, y=0),
        implementation=ReactiveImplementationModelInput.WITHLATEST,
        defaults={},
        constream=[],
        instream=[[StreamItemFragment(key=1, kind=StreamKind.INT, nullable=False)]],
        outstream=[[StreamItemFragment(key=1, kind=StreamKind.INT, nullable=False)]],
    )


@pytest.fixture
def reactive_chunk_node_with_defaults():
    return FlowNodeFragmentBaseReactiveNode(
        id=1,
        position=FlowNodeFragmentBasePosition(x=0, y=0),
        implementation=ReactiveImplementationModelInput.CHUNK,
        defaults={"sleep": 1000},
        constream=[],
        instream=[[StreamItemFragment(key=1, kind=StreamKind.INT, nullable=False)]],
        outstream=[[StreamItemFragment(key=1, kind=StreamKind.INT, nullable=False)]],
    )


@pytest.fixture
def reactive_chunk_node():
    return FlowNodeFragmentBaseReactiveNode(
        id=1,
        position=FlowNodeFragmentBasePosition(x=0, y=0),
        implementation=ReactiveImplementationModelInput.CHUNK,
        defaults={},
        constream=[],
        instream=[[StreamItemFragment(key=1, kind=StreamKind.INT, nullable=False)]],
        outstream=[[StreamItemFragment(key=1, kind=StreamKind.INT, nullable=False)]],
    )
