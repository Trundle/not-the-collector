=========
Collector
=========

This is NOT the Graylog collector. It is a clone written in `Haskell
<https://www.haskell.org/>`_.


Usage
=====

Configuration
-------------

.. code:: toml

   # Sample configuration file for collector
   #
   # collector uses the toml configuration format. See
   # https://github.com/toml-lang/toml for details.
   #
   
   [inputs]
    [inputs.config-sample]
    type = "file"
    path = "/file/to/watch"
    outputs = ["stdout", "gelf-udp"]
   
   [outputs]
     [outputs.gelf-udp]
     type = "gelf-udp"
     host = "127.0.0.1"
     port = 12201
     client-queue-size = 512
   
     [outputs.stdout]
     type = "stdout"


Running the Collector
---------------------

The collector needs a configuration file and can be started with the following command::

   $ collector run -f collector.toml


Building
========

Use `stack <https://github.com/commercialhaskell/stack>`_::

   stack build


License
=======

MIT/Expat. See ``LICENSE`` for details.
