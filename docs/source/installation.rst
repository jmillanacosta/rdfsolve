Installation
============
The most recent release can be installed from
`PyPI <https://pypi.org/project/rdfsolve>`_ with:

.. code-block:: shell

    uv pip install rdfsolve

This installs the core: opening and writing schemas, the client and conversion rules. The
other parts are extras:

- ``rdfsolve[mining]``: mining local indexes at scale (polars);
- ``rdfsolve[exports]``: LinkML exports;
- ``rdfsolve[mappings]``: SSSOM mapping sets;
- ``rdfsolve[cli]``: the command line;
- ``rdfsolve[web]``: the web service;
- ``rdfsolve[validation]``: SHACL validation with pySHACL;
- ``rdfsolve[all]``: all of these.

The most recent code and data can be installed directly from GitHub with:

.. code-block:: shell

    uv pip install git+https://github.com/jmillanacosta/rdfsolve.git

To install in development mode, use the following:

.. code-block:: shell

    git clone git+https://github.com/jmillanacosta/rdfsolve.git
    cd rdfsolve
    uv pip install -e .
