from taskiq.exceptions import ResultBackendError, ResultGetError, TaskiqError


class TaskIQValkeyError(TaskiqError):
    """Base error for all taskiq-valkey exceptions."""


class DuplicateExpireTimeSelectedError(ResultBackendError, TaskIQValkeyError):
    """Error if two lifetimes are selected."""

    __template__ = "Choose either result_ex_time or result_px_time."


class ExpireTimeMustBeMoreThanZeroError(ResultBackendError, TaskIQValkeyError):
    """Error if two lifetimes are less or equal zero."""

    __template__ = (
        "You must select one expire time param and it must be more than zero."
    )


class ResultIsMissingError(TaskIQValkeyError, ResultGetError):
    """Error if there is no result when trying to get it."""
