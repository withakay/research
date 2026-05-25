from enum import StrEnum


class AdminActionStatus(StrEnum):
    SUCCESS = "success"
    NOT_FOUND = "not_found"
    WRONG_STATE = "wrong_state"

    @property
    def succeeded(self) -> bool:
        return self is AdminActionStatus.SUCCESS
