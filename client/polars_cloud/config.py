from __future__ import annotations

import contextlib
import json
import os
from pathlib import Path
from typing import TYPE_CHECKING, Callable, TypedDict

from polars._utils.various import normalize_filepath

if TYPE_CHECKING:
    import sys
    from types import TracebackType

    if sys.version_info >= (3, 11):
        from typing import Self, Unpack
    else:
        from typing_extensions import Self, Unpack

_SINGLE_NODE = "POLARS_CLOUD_SINGLE_NODE"
_USER_NAME = "POLARS_CLOUD_USER_NAME"

# note: register all Config-specific environment variable names here; need to constrain
# which 'POLARS_CLOUD_' environment variables are recognized, as there may be
# other lower-level and/or unstable settings that should not be saved or reset
# with the Config vars.
_POLARS_CLOUD_CFG_ENV_VARS = {_SINGLE_NODE, _USER_NAME}


class ConfigParameters(TypedDict, total=False):
    """Parameters supported by the Polars Cloud Config."""

    single_node: bool | None
    username: str | None


# vars that set the rust env directly should declare themselves here as the Config
# method name paired with a callable that returns the current state of that value:
with contextlib.suppress(ImportError, NameError):
    # TODO: set when we have defaults from rust lib
    _POLARS_CLOUD_CFG_DIRECT_VARS: dict[str, Callable[[], str]] = {}


class Config(contextlib.ContextDecorator):
    """Configure polars_cloud; offers options for scaling modes and more.

    Notes
    -----
    Can also be used as a context manager OR a function decorator in order to
    temporarily scope the lifetime of specific options. For example:

    >>> with pl.Config() as cfg:
    ...     # set verbose for more detailed output within the scope
    ...     cfg.set_verbose(True)  # doctest: +IGNORE_RESULT
    >>> # scope exit - no longer in verbose mode

    This can also be written more compactly as:

    >>> with pl.Config(verbose=True):
    ...     pass

    (The compact format is available for all `Config` methods that take a single value).

    Alternatively, you can use as a decorator in order to scope the duration of the
    selected options to a specific function:

    >>> @pl.Config(verbose=True)
    ... def test():
    ...     pass
    """

    _context_options: ConfigParameters | None = None
    _original_state: str = ""

    def __init__(
        self,
        *,
        restore_defaults: bool = False,
        apply_on_context_enter: bool = False,
        **options: Unpack[ConfigParameters],
    ) -> None:
        """Initialise a Config object instance for context manager usage.

        Any `options` kwargs should correspond to the available named "set_*"
        methods, but are allowed to omit the "set_" prefix for brevity.

        Parameters
        ----------
        restore_defaults
            set all options to their default values (this is applied before
            setting any other options).
        apply_on_context_enter
            defer applying the options until a context is entered. This allows you
            to create multiple `Config` instances with different options, and then
            reuse them independently as context managers or function decorators
            with specific bundles of parameters.
        **options
            keyword args that will set the option; equivalent to calling the
            named "set_<option>" method with the given value.

        """
        # save original state _before_ any changes are made
        self._original_state = self.save()
        if restore_defaults:
            self.restore_defaults()

        if apply_on_context_enter:
            # defer setting options; apply only on entering a new context
            self._context_options = options
        else:
            # apply the given options immediately
            self._set_config_params(**options)
            self._context_options = None

    def __enter__(self) -> Self:
        """Support setting Config options that are reset on scope exit."""
        self._original_state = self._original_state or self.save()
        if self._context_options:
            self._set_config_params(**self._context_options)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Reset any Config options that were set within the scope."""
        self.restore_defaults().load(self._original_state)
        self._original_state = ""

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Config):
            return False
        return (self._original_state == other._original_state) and (
            self._context_options == other._context_options
        )

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    def _set_config_params(self, **options: Unpack[ConfigParameters]) -> None:
        for opt, value in options.items():
            if not hasattr(self, opt) and not opt.startswith("set_"):
                opt = f"set_{opt}"
            if not hasattr(self, opt):
                msg = f"`Config` has no option {opt!r}"
                raise AttributeError(msg)
            getattr(self, opt)(value)

    @classmethod
    def load(cls, cfg: str) -> Config:
        """Load (and set) previously saved Config options from a JSON string.

        Parameters
        ----------
        cfg : str
            JSON string produced by `Config.save()`.

        See Also
        --------
        load_from_file : Load (and set) Config options from a JSON file.
        save : Save the current set of Config options as a JSON string or file.
        """
        try:
            options = json.loads(cfg)
        except json.JSONDecodeError as err:
            msg = "invalid Config string (did you mean to use `load_from_file`?)"
            raise ValueError(msg) from err

        cfg_load = Config()
        opts = options.get("environment", {})
        for key, opt in opts.items():
            if opt is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = opt

        for cfg_methodname, value in options.get("direct", {}).items():
            if hasattr(cfg_load, cfg_methodname):
                getattr(cfg_load, cfg_methodname)(value)
        return cfg_load

    @classmethod
    def load_from_file(cls, file: Path | str) -> Config:
        """Load (and set) previously saved Config options from file.

        Parameters
        ----------
        file : Path | str
            File path to a JSON string produced by `Config.save()`.

        See Also
        --------
        load : Load (and set) Config options from a JSON string.
        save : Save the current set of Config options as a JSON string or file.
        """
        try:
            options = Path(normalize_filepath(file)).read_text()
        except OSError as err:
            msg = f"invalid Config file (did you mean to use `load`?)\n{err}"
            raise ValueError(msg) from err

        return cls.load(options)

    @classmethod
    def restore_defaults(cls) -> type[Config]:
        """Reset all polars Config settings to their default state.

        Notes
        -----
        This method operates by removing all Config options from the environment,
        and then setting any local (non-env) options back to their default value.

        Examples
        --------
        >>> cfg = pl.Config.restore_defaults()  # doctest: +SKIP
        """
        # unset all Config environment variables
        for var in _POLARS_CLOUD_CFG_ENV_VARS:
            os.environ.pop(var, None)

        # reset all 'direct' defaults
        for method in _POLARS_CLOUD_CFG_DIRECT_VARS:
            getattr(cls, method)(None)

        return cls

    @classmethod
    def save(cls, *, if_set: bool = False) -> str:
        """Save the current set of Config options as a JSON string.

        Parameters
        ----------
        if_set
            By default this will save the state of all configuration options; set
            to `False` to save only those that have been set to a non-default value.

        See Also
        --------
        load : Load (and set) Config options from a JSON string.
        load_from_file : Load (and set) Config options from a JSON file.
        save_to_file : Save the current set of Config options as a JSON file.

        Examples
        --------
        >>> json_state = pl.Config.save()

        Returns
        -------
        str
            JSON string containing current Config options.
        """
        environment_vars = {
            key: os.environ.get(key)
            for key in sorted(_POLARS_CLOUD_CFG_ENV_VARS)
            if not if_set or (os.environ.get(key) is not None)
        }
        direct_vars = {
            cfg_methodname: get_value()
            for cfg_methodname, get_value in _POLARS_CLOUD_CFG_DIRECT_VARS.items()
        }
        options = json.dumps(
            {"environment": environment_vars, "direct": direct_vars},
            separators=(",", ":"),
        )
        return options

    @classmethod
    def save_to_file(cls, file: Path | str) -> None:
        """Save the current set of Config options as a JSON file.

        Parameters
        ----------
        file
            Optional path to a file into which the JSON string will be written.
            Leave as `None` to return the JSON string directly.

        See Also
        --------
        load : Load (and set) Config options from a JSON string.
        load_from_file : Load (and set) Config options from a JSON file.
        save : Save the current set of Config options as a JSON string.

        Examples
        --------
        >>> pl.Config().save_to_file("~/polars/config.json")  # doctest: +SKIP
        """
        file = Path(normalize_filepath(file)).resolve()
        file.write_text(cls.save())

    @classmethod
    def state(
        cls, *, if_set: bool = False, env_only: bool = False
    ) -> dict[str, str | None]:
        """Show the current state of all Config variables in the environment as a dict.

        Parameters
        ----------
        if_set
            By default this will show the state of all `Config` environment variables.
            change this to `True` to restrict the returned dictionary to include only
            those that have been set to a specific value.
        env_only
            Include only Config environment variables in the output; some options (such
            as "set_fmt_float") are set directly, not via an environment variable.

        Examples
        --------
        >>> set_state = pl.Config.state(if_set=True)
        >>> all_state = pl.Config.state()
        """
        config_state = {
            var: os.environ.get(var)
            for var in sorted(_POLARS_CLOUD_CFG_ENV_VARS)
            if not if_set or (os.environ.get(var) is not None)
        }
        if not env_only:
            for cfg_methodname, get_value in _POLARS_CLOUD_CFG_DIRECT_VARS.items():
                config_state[cfg_methodname] = get_value()

        return config_state

    @classmethod
    def set_single_node(cls, active: bool | None = True) -> type[Config]:  # noqa: FBT001
        """Enable single node scaling mode independent of cluster state.

        Examples
        --------
        Set globally
        >>> pl.Config.set_single_node(True)  # doctest: +SKIP
        >>> do_polars_operations()

        Or set as context manager
        >>> with pl.Config(single_node=True):  # doctest: +SKIP
        ...     do_polars_operations()
        """
        if active is None:
            os.environ.pop(_SINGLE_NODE, None)
        else:
            os.environ[_SINGLE_NODE] = str(int(active))
        return cls

    @classmethod
    def set_user_name(cls, name: str) -> type[Config]:
        """Set the username for user identification in insecure cluster environments."""
        if name is None:
            os.environ.pop(_USER_NAME, None)
        else:
            os.environ[_USER_NAME] = name
        return cls

    @classmethod
    def get(cls, name: str) -> str | None:
        return os.environ.get(name)

    @classmethod
    def _is_set(cls, attr: str, expected: str) -> bool:
        var = os.environ.get(attr)
        return var is not None and var == expected
