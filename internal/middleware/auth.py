from fastapi import Header, HTTPException, status


def verify_token(authorization: str | None = Header(default=None)) -> None:
    if authorization != "Bearer test-token":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized",
        )
