from typing import List
from rekuest.api.schema import NodeKindInput
from fluss.api.schema import (
    FlowFragmentGraph,
    FlowNodeFragmentBaseArkitektNode,
    FlowNodeFragmentBaseReactiveNode,
    ReactiveImplementationModelInput,
)
from .events import OutEvent, InEvent
import pydantic
from .errors import FlowLogicError


def connected_events(
    graph: FlowFragmentGraph, event: OutEvent, t: int
) -> List[InEvent]:
    events = []

    for edge in graph.edges:
        if edge.source == event.source and edge.source_handle == event.handle:
            try:
                events.append(
                    InEvent(
                        target=edge.target,
                        handle=edge.target_handle,
                        type=event.type,
                        value=event.value,
                        current_t=t,
                    )
                )
            except pydantic.ValidationError as e:
                raise FlowLogicError(f"Invalid event for {edge} : {event}") from e

    return events


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
