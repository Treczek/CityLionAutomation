import time
from typing import Coroutine, Callable

import structlog
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException

from webserver.config.settings import get_settings
from webserver.routes.routes import root
from webserver.utils import create_response
from webserver.utils.logging import setup_logging


def get_application() -> FastAPI:
    config = get_settings("development")
    application = FastAPI(title="Matchmaking - MLOPS", debug=config.debug)
    setup_logging(verbosity=config.verbosity)
    logger = structlog.getLogger(__name__)

    @application.middleware("http")
    async def log_on_request(
        request: Request, call_next: Callable[[Request], Coroutine]
    ) -> JSONResponse:
        logger.info("request-received", path=request.url, method=request.method)
        response = await call_next(request)
        return response

    @application.middleware("http")
    async def add_process_time(
        request: Request, call_next: Callable[[Request], Coroutine]
    ) -> JSONResponse:
        start_time = time.time()
        response = await call_next(request)
        response.headers["process-time"] = str(time.time() - start_time)
        return response

    @application.exception_handler(HTTPException)
    async def handle_http_error(
        request: Request, exception: HTTPException
    ) -> JSONResponse:
        logger.error(
            "http-error",
            detail=str(exception.detail),
            status_code=exception.status_code,
        )
        return create_response(exception.detail, error_code=exception.status_code)

    @application.exception_handler(Exception)
    async def handle_exception(request: Request, exception: Exception) -> JSONResponse:
        logger.error("unexpected-error", detail=str(exception))
        return create_response(str(exception), error_code=500, add_stacktrace=True)

    application.include_router(root)

    return application


app = get_application()
