from typing import List, Tuple, Union
from pydantic import BaseModel, Field
from enum import Enum


class EventType(str, Enum):
    NEXT = "NEXT"
    ERROR = "ERROR"
    COMPLETE = "COMPLETE"


Returns = List


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

    def to_state(self):
        return {
            "source": self.source,
            "handle": self.handle,
            "type": self.type,
            "value": str(self.value),
        }

    class Config:
        arbitrary_types_allowed = True
