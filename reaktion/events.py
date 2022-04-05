from typing import Tuple, Union
from prometheus_client import Enum
from pydantic import BaseModel, Field


class EventType(str, Enum):
    NEXT = "next"
    ERROR = "error"
    COMPLETE = "complete"


Returns = Tuple


class InEvent(BaseModel):
    target: str
    """The node that is targeted by the event"""
    handle: str = Field(..., description="The handle of the port")
    """ The handle of the port that emitted the event"""
    type: EventType = Field(..., description="The event type")
    """ The type of event"""
    value: Union[Exception, Returns] = Field(
        None, description="The value of the event (null, exception or any"
    )
    """ The attached value of the event"""

    class Config:
        arbitrary_types_allowed = True


class OutEvent(BaseModel):
    source: str
    """ The node that emitted the event """
    handle: str = Field(..., description="The handle of the port")
    """ The handle of the port that emitted the event"""
    type: EventType = Field(..., description="The event type")
    """ The type of event"""
    value: Union[Exception, Returns] = Field(
        None, description="The value of the event (null, exception or any"
    )
    """ The attached value of the event"""

    class Config:
        arbitrary_types_allowed = True
