from arkitekt.messages.postman.reserve.reserve_transition import ReserveState
from arkitekt.messages.postman.provide.provide_transition import ProvideState
from typing import List, Union
from pydantic.main import BaseModel


class ProvideEvent(BaseModel):
    diagram_id: str


class TransitionEvent(ProvideEvent):
    type: str = "TRANSITION"
    state: ReserveState
    message: str
    reservation: str


class Event(BaseModel):
    diagram_id: str
    handle: str
    pass


class DoneEvent(Event):
    type: str = "DONE"
    """When a Node sends a Done event it will no longer
    send any return

    Args:
        BaseModel ([type]): [description]
    """


class CancelEvent(Event):
    type: str = "CANCEL"
    """When a Node sends a Done event it will no longer
    send any return

    Args:
        BaseModel ([type]): [description]
    """


class YieldEvent(Event):
    type: str = "YIELD"
    """When a Node sends a Done event it will no longer
    send any return

    Args:
        BaseModel ([type]): [description]
    """
    returns: List


class ReturnEvent(Event):
    type: str = "RETURN"
    """When a Node sends a Done event it will no longer
    send any return

    Args:
        BaseModel ([type]): [description]
    """
    returns: List


class ErrorEvent(Event):
    type: str = "ERROR"
    throwing: str  # the node that threw the event
    exception: str
    message: str


class SkipEvent(Event):
    type: str = "SKIP"
    skipper: str  # the node that skipped a beat
    """When a Node sends a Done event it will no longer
    send any return

    Args:
        BaseModel ([type]): [description]
    """


EventType = Union[DoneEvent, YieldEvent, ReturnEvent, ErrorEvent]
