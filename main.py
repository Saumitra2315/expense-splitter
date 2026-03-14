from fastapi import FastAPI

from internal.handler.expense_handler import router as expense_router
from internal.handler.group_handler import router as group_router

app = FastAPI(title="Expense Splitter")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


app.include_router(group_router)
app.include_router(expense_router)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8080)
