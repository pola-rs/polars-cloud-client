======
Config
======
.. currentmodule:: polars_cloud

Config options
--------------

.. autosummary::
   :toctree: api/

    Config.set_single_node

Config load, save, state
------------------------
.. autosummary::
   :toctree: api/

    Config.load
    Config.load_from_file
    Config.save
    Config.save_to_file
    Config.state
    Config.restore_defaults

While it is easy to restore *all* configuration options to their default
value using ``restore_defaults``, it can also be useful to reset *individual*
options. This can be done by setting the related value to ``None``, eg:

.. code-block:: python

    pl.Config.set_single_node(None)


Use as a context manager
------------------------

Note that ``Config`` supports setting context-scoped options. These options
are valid *only* during scope lifetime, and are reset to their initial values
(whatever they were before entering the new context) on scope exit.

You can take advantage of this by initialising a ``Config`` instance and then
explicitly calling one or more of the available "set\_" methods on it...

.. code-block:: python

    with pc.Config() as cfg:
        cfg.set_single_node(True)
        do_various_things()

    # on scope exit any modified settings are restored to their previous state

...or, often cleaner, by setting the options in the ``Config`` init directly
(optionally omitting the "set\_" prefix for brevity):

.. code-block:: python

    with pc.Config(single_node=True):
        do_various_things()

Use as a decorator
------------------

In the same vein, you can also use a ``Config`` instance as a function decorator
to temporarily set options for the duration of the function call:

.. code-block:: python

    cfg_single_node = pc.Config(single_node=True, apply_on_context_enter=True)

    @cfg_single_node
    def run_remote_lazyframe(lf: pl.LazyFrame) -> None:
        lf.remote().execute()
