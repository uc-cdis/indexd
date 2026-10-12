from authutils.token.fastapi import access_token
from fastapi import Depends, HTTPException, Security
from fastapi.security import (
    HTTPAuthorizationCredentials,
    HTTPBearer,
    HTTPBasic,
    HTTPBasicCredentials,
)
from starlette.requests import Request
from starlette.status import HTTP_401_UNAUTHORIZED, HTTP_403_FORBIDDEN

from gen3authz.client.arborist.errors import ArboristError

from cdislogging import get_logger

from indexd.auth.errors import AuthError

logger = get_logger(__name__)

# auto_error=False prevents FastAPI from raising a 403 when the request
# is missing an Authorization header. Instead, we want to return a 401
# to signify that we did not recieve valid credentials
bearer = HTTPBearer(auto_error=False)
basic = HTTPBasic(auto_error=False)


class Auth:
    def __init__(
        self,
        request: Request,
        bearer_token: HTTPAuthorizationCredentials = Security(bearer),
        basic_creds: HTTPBasicCredentials = Security(basic),
    ):
        self.arborist = request.app.arborist_client
        self.auth_driver = request.app.auth

        self.basic_auth = basic_creds
        self.bearer_token = bearer_token

    async def authz(self, token: str, method: str, resource: list) -> None:
        """
        Check `method` access on `resource` for service 'indexd'.

        Returns None if the request is authorized, raises AuthError otherwise.
        """
        if not self.arborist:
            raise AuthError(
                "Arborist is not configured; cannot perform authorization check"
            )

        async def auth_request(resources: list) -> bool:
            # a successful call to arborist returns a bool, otherwise it raises
            # an ArboristError, which we treat as unauthorized
            try:
                return await self.arborist.auth_request(
                    token, "indexd", method, resources
                )
            except ArboristError as e:
                logger.error(
                    f"Request to Arborist failed; assuming unauthorized. Details:\n{e}"
                )
                return False

        if await auth_request(resource):
            return

        # admins can perform all operations
        is_admin = await auth_request(["/services/indexd/admin"])
        if not is_admin and not resource:
            # if `authz` is empty (no `resource`), admin == access to
            # `/programs` (deprecated - for backwards compatibility).
            is_admin = await auth_request(["/programs"])
            if is_admin:
                logger.warning(
                    "The indexd admin '/programs' logic is deprecated. Please update your policy to '/services/indexd/admin'"
                )

        if not is_admin:
            raise AuthError("Permission denied.")

    async def get_token_claims(self) -> dict:
        if not self.bearer_token:
            err_msg = "Must provide an access token."
            logger.error(err_msg)
            raise HTTPException(
                HTTP_401_UNAUTHORIZED,
                err_msg,
            )

        try:
            # NOTE: token can be None if no Authorization header was provided, we
            # expect this to cause a downstream exception since it is invalid
            token_claims = await access_token("user", "openid", purpose="access")(
                self.bearer_token
            )
        except Exception as e:
            logger.error(
                f"Could not get token claims:\n{e.detail if hasattr(e, 'detail') else e}",
                exc_info=True,
            )
            raise HTTPException(
                HTTP_401_UNAUTHORIZED,
                "Could not verify, parse, and/or validate scope from provided access token.",
            )

        return token_claims

    async def authorize(
        self,
        method: str,
        resources: list,
        throw: bool = True,
    ) -> bool:
        if self.basic_auth:
            try:
                await self.auth_driver.auth(
                    self.basic_auth.username,
                    self.basic_auth.password,
                )
                return True

            except AuthError as e:
                logger.error(
                    "Basic Authorization failed: Invalid username or password."
                )
                if throw:
                    raise HTTPException(
                        HTTP_401_UNAUTHORIZED, "Invalid username or password."
                    )
                return False

        token = (
            self.bearer_token.credentials
            if self.bearer_token and hasattr(self.bearer_token, "credentials")
            else None
        )

        try:
            await self.authz(token, method, resources)
            authorized = True
        except (AuthError, ArboristError) as e:
            logger.error(f"Authorization check failed: {e}")
            authorized = False

        if not authorized:
            logger.error(
                f"Authorization error: user must have '{method}' access on {resources} for service 'audit'."
            )
            if throw:
                if not self.basic_auth and not self.bearer_token:
                    raise HTTPException(
                        status_code=HTTP_403_FORBIDDEN,
                        detail="Authentication required",
                        headers={"WWW-Authenticate": "Basic"},
                    )
                raise HTTPException(
                    HTTP_403_FORBIDDEN,
                    "Permission denied",
                )

        return authorized


async def require_basic_auth(auth: Auth = Depends(Auth)) -> Auth:
    """
    Require valid HTTP Basic credentials, without consulting Arborist.

    The deprecated `/alias` endpoints authenticated with a username/password pair
    checked directly against the auth driver. Both a missing `Authorization` header
    and an incorrect username/password raise AuthError, which the app maps to 403.
    """
    if not auth.basic_auth:
        err_msg = "Username / password required."
        logger.error(err_msg)
        raise AuthError(err_msg)

    # raises AuthError on an unknown user or a password mismatch
    await auth.auth_driver.auth(auth.basic_auth.username, auth.basic_auth.password)

    return auth
