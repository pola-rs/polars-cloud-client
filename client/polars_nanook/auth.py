from __future__ import annotations

from polars_nanook import constants
from polars_nanook._tracing import traced


@traced
def authenticate(
    *,
    client_id: str | None = None,
    client_secret: str | None = None,
    interactive: bool = True,
) -> None:
    """Authenticate with Polars Nanook.

    This will attempt to authenticate with Polars Nanook or redirect to `login`
    if no valid token is found. If the `interactive` flag is set to false
    the function will fail if no valid token was found without redirecting to `login`.

    Parameters
    ----------
    client_id
        Optional client id for service account authentication
    client_secret
        Optional client secret for service account authentication
    interactive
        Fail in case no valid authentication method was found without redirecting
        to interactive login

    See Also
    --------
    login: Login will force a new interactive login without using cached tokens

    Examples
    --------
    >>> pn.authenticate()  # doctest: +SKIP
    >>> pn.authenticate(
    ...     client_id="CLIENT_ID", client_secret="CLIENT_SECRET"
    ... )  # doctest: +SKIP
    """
    constants.API_CLIENT.authenticate(client_id, client_secret, interactive)


@traced
def login() -> None:
    """Login interactively to Polars Nanook.

    This will open up a browser window where you can login
    and grant Polars Nanook the needed access rights.

    For machine access to Polars Nanook, you must set up service accounts.
    See: https://docs.pola.rs/polars-cloud/explain/service-accounts/

    See Also
    --------
    authenticate: Authenticate with existing token

    Examples
    --------
    >>> pn.login()  # doctest: +SKIP
    """
    constants.API_CLIENT.login()
