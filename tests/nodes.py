import json
import pytest
from fluss.api.schema import (
    FlowFragment,
    ArkitektNodeFragment,
    ReactiveNodeFragment,
    TemplateNodeFragment,
    RunMutationStart,
    SnapshotMutationSnapshot,
    TrackMutationTrack,
    FlowNodeFragmentBaseArkitektNode,
    FlowNodeFragmentBasePosition,
    FlowNodeFragmentBaseReactiveNode,
    StreamItemFragment,
    ReactiveImplementationModelInput,
    StreamKind,
)
from rekuest.api.schema import NodeKind


@pytest.fixture
def arkitekt_generator_node():
    return FlowNodeFragmentBaseArkitektNode(
        id=1,
        position=FlowNodeFragmentBasePosition(x=0, y=0),
        name="add_generator",
        kind=NodeKind.GENERATOR,
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
        kind=NodeKind.FUNCTION,
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
