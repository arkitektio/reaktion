from fluss.api.schema import FlowNodeFragmentBaseReactiveNode
from .base import Atom


class ReactiveAtom(Atom):
    node: FlowNodeFragmentBaseReactiveNode
