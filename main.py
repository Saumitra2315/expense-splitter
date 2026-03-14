from fastapi import FastAPI
from fastapi.responses import JSONResponse

from internal.handler.expense_handler import router as expense_router
from internal.handler.group_handler import router as group_router
from internal.service.ledger_service import ServiceError

app = FastAPI(title="Expense Splitter")


@app.exception_handler(ServiceError)
async def service_error_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": str(exc)},
    )


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


app.include_router(group_router)
app.include_router(expense_router)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8080)
