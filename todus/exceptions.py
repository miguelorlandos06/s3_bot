from __future__ import annotations
class ToDusError(Exception):
    pass
class ToDusConnectionError(ToDusError):
    pass
class ToDusTimeoutError(ToDusError):
    pass
class ToDusNotFoundError(ToDusError):
    pass
class ToDusAlreadyExistsError(ToDusError):
    pass
class ToDusPermissionError(ToDusError):
    pass
class ToDusServerError(ToDusError):
    def __init__(self, status_code: int, message: str = ""):
        self.status_code = status_code
        super().__init__(f"HTTP {status_code}: {message}")
class ToDusParseError(ToDusError):
    pass
class NamespaceError(ToDusError):
    pass
class NamespaceNotFoundError(NamespaceError):
    pass
class NamespaceAlreadyExistsError(NamespaceError):
    pass
class FileNotFoundError(ToDusNotFoundError):
    pass
