from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class RpcExecutionResult:
    success: bool
    response_body: Optional[Any] = None
    error: Optional[str] = None
    status_code: Optional[int] = None
