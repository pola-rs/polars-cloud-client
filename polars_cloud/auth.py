from __future__ import annotations

from polars_cloud import constants


def authenticate(
    *,
    client_id: str | None = None,
    client_secret: str | None = None,
    interactive: bool = True,
) -> None:
    """Authenticate with Polars Cloud.

    This will attempt to authenticate with Polars Cloud or redirect to `login`
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
    >>> pc.authenticate()  # doctest: +SKIP
    >>> pc.authenticate(
    ...     client_id="CLIENT_ID", client_secret="CLIENT_SECRET"
    ... )  # doctest: +SKIP
    """
    constants.API_CLIENT.authenticate(client_id, client_secret, interactive)


def login() -> None:
    """Login interactively to Polars Cloud.

    This will open up a browser window where you can login
    and grant Polars Cloud the needed access rights.

    For machine access to Polars Cloud, you must set up service accounts.
    See: https://docs.pola.rs/polars-cloud/explain/service-accounts/

    See Also
    --------
    authenticate: Authenticate with existing token

    Examples
    --------
    >>> pc.login()  # doctest: +SKIP
    """
    constants.API_CLIENT.login()
