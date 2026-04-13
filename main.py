from fastapi import FastAPI
from fastapi.responses import JSONResponse

from internal.handler.budget_handler import router as budget_router
from internal.handler.expense_handler import router as expense_router
from internal.handler.export_handler import router as export_router
from internal.handler.group_handler import router as group_router
from internal.handler.notification_handler import router as notification_router
from internal.middleware.rate_limiter import RateLimiterMiddleware, RateLimitConfig
from internal.middleware.request_logger import RequestLoggerMiddleware
from internal.service.ledger_service import ServiceError

app = FastAPI(
    title="SettleUp API",
    description="Production-inspired FastAPI ledger service for shared expenses",
    version="2.1.0",
)


@app.exception_handler(ServiceError)
async def service_error_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": str(exc)},
    )


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


# Register routers.
app.include_router(group_router)
app.include_router(expense_router)
app.include_router(notification_router)
app.include_router(export_router)
app.include_router(budget_router)

# Add middleware (order matters: last added = outermost).
app.add_middleware(RequestLoggerMiddleware)
app.add_middleware(
    RateLimiterMiddleware,
    config=RateLimitConfig(requests_per_minute=120, burst_size=20),
)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8080)
