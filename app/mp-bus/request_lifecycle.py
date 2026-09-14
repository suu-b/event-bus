from enum import Enum

class Status(str, Enum):
    CREATED = "CREATED"
    PENDING = "PENDING"
    PICKED_UP = "PICKED_UP"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    EXPIRED = "EXPIRED"