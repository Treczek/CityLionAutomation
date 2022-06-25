import traceback

import structlog
from fastapi.responses import JSONResponse

logger = structlog.getLogger(__name__)


def create_response(
    data, error_code: int, add_stacktrace: bool = False
) -> JSONResponse:
    if not error_code:
        return JSONResponse({**data, "IsSuccess": True})
    else:
        logger.warning("response-error", data=data, code=error_code)
        if add_stacktrace:
            print(traceback.format_exc())
        return JSONResponse({"Details": data, "IsSuccess": False, "Error": error_code})
