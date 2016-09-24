#!/usr/bin/env python3
#Will execute the unit tests manually and provide a better report than tox
import subprocess
import os
from pprint import pprint
import argparse
PYS = ['3.3', '3.3', '3.4']
SOLRS = ['5.2.1', '5.5.3', '6.1.0', '6.2.1']
TESTS = ['test_client', 'test_indexq', 'test_reindexer', 'test_resp',
         'test_collections', 'test_zk']

parser = argparse.ArgumentParser()
parser.add_argument('-test', type=str, nargs='*', help='Which test.')
parser.add_argument('-solr', type=str, nargs='*', help='Which Solr.')
parser.add_argument('-py', type=str, nargs='*', help='PY')
args = parser.parse_args()

if args.test:
    TESTS = args.test
if args.solr:
    SOLRS = args.solr
if args.py:
    PYS = args.py


print("Starting Testing")
REPORT = {}
for ver in PYS:
    res = subprocess.call(["python3", "/usr/local/bin/tox", "-e", "py{}".format(ver.replace('.',''))])
    python = '.tox/py{}/bin/python{}'.format(ver.replace('.', ''), ver)
    REPORT[ver] = {}
    for solr in SOLRS:
        REPORT[ver][solr] = {}
        os.environ['SOLR_TEST_URL'] = 'http://localhost:9{}/solr/'.format(solr.replace('.',''))
        for test in TESTS:
            print("Running: {}- {}- {}".format(ver, solr, test))
            REPORT[ver][solr][test] = subprocess.call([python, "-m", "unittest", "test.{}".format(test)])
pprint(REPORT)
