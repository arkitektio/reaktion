from typing import List
from rekuest.api.schema import NodeKindInput
from fluss.api.schema import (
    FlowFragment,
    FlowFragmentGraph,
    FlowNodeFragmentBaseArkitektNode,
    FlowNodeFragmentBaseReactiveNode,
    ReactiveImplementationModelInput,
)
from .events import OutEvent, InEvent


def connected_events(graph: FlowFragmentGraph, event: OutEvent) -> List[InEvent]:
    return [
        InEvent(
            target=edge.target,
            handle=edge.target_handle,
            type=event.type,
            value=event.value,
        )
        for edge in graph.edges
        if edge.source == event.source and edge.source_handle == event.handle
    ]


def infer_kind_from_graph(graph: FlowFragmentGraph) -> NodeKindInput:

    kind = NodeKindInput.FUNCTION

    for node in graph.nodes:
        if isinstance(node, FlowNodeFragmentBaseArkitektNode):
            if node.kind == NodeKindInput.GENERATOR:
                kind = NodeKindInput.GENERATOR
                break
        if isinstance(node, FlowNodeFragmentBaseReactiveNode):
            if node.implementation == ReactiveImplementationModelInput.CHUNK:
                kind = NodeKindInput.GENERATOR
                break

    return kind
