#!/bin/bash

python3 -m unittest test.test_client
python3 -m unittest test.test_resp
python3 -m unittest test.test_indexq
python3 -m unittest test.test_collections
python3 -m unittest test.test_reindexer
