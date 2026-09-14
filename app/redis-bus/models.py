from pydantic import BaseModel, Field
from typing import Literal
import time

from request_lifecycle import Status

class Request(BaseModel):
    id: str
    client_id: str
    content: str
    status: Status = Status.CREATED
    timestamp: float = Field(default_factory=time.time)
    picked_up_by: str | None = None

class App_Channels(BaseModel):
    request_channel: str
