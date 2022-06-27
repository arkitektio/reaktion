from typing import List
from fluss.api.schema import FlowFragment, FlowFragmentGraph
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
