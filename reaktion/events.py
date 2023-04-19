from typing import List, Tuple, Union, List, Tuple, Any
from pydantic import BaseModel, Field, validator
from enum import Enum


class EventType(str, Enum):
    NEXT = "NEXT"
    ERROR = "ERROR"
    COMPLETE = "COMPLETE"


Returns = Tuple[Any, ...]


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
    current_t: int
    """ The current (in loop) time of the event"""

    @validator("handle")
    def validate_handle(cls, v):
        if isinstance(v, int):
            v = f"arg_{v}"

        if v.startswith("return_"):
            raise ValueError(f"Handle needs to start with arg_. This is an inevent {v}")
        if not v.startswith("arg_"):
            raise ValueError(
                f"Handle needs to start with arg_. This is an outevent {v} "
            )

        return v

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
    caused_by: List[int]
    """ The attached value of the event"""

    @validator("handle")
    def validate_handle(cls, v):
        if isinstance(v, int):
            v = f"return_{v}"

        if v.startswith("arg_"):
            raise ValueError(f"Handle cannot start with arg_. This is an outevent {v}")
        if not v.startswith("return_"):
            raise ValueError(
                f"Handle needs to start with return_. This is an outevent {v}"
            )

        return v

    def to_state(self):
        if self.value:
            value = (
                self.value if not isinstance(self.value, Exception) else str(self.value)
            )
        else:
            value = None

        return {
            "source": self.source,
            "handle": self.handle,
            "type": self.type,
            "value": value,
        }

    class Config:
        arbitrary_types_allowed = True
