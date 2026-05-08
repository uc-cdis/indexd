from fastapi import Depends, HTTPException, status, Request
from .errors import AuthError
from ..errors import UserError


def authorize_decorator(method=None, resources=None):
    """
    Replaces the request authorization with a user context.
    Raises AuthError if authorization fails.

    If called with (method, resources), it will check with Arborist if HTTP Basic Auth is
    not present, or fallback to the previous check.
    """

    async def dependency(request: Request):
        auth = request.headers.get("Authorization")
        if not auth:
            raise AuthError("Username / password required.")

        try:
            type_, credentials = auth.split(" ")
            if type_.lower() != "basic":
                raise ValueError
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Malformed Authorization header.",
            )

        import base64

        username, password = base64.b64decode(credentials).decode().split(":", 1)

        # Replace `current_app.auth.auth` with FastAPI auth logic

        if method and resources:
            if not isinstance(resources, list):
                raise UserError(f"'authz' must be a list, received '{resources}'.")
            # Replace with: your_auth_service.authz(method, list(set(resources)))

        # If authorization fails, raise AuthError or HTTPException as needed

    return Depends(dependency)
