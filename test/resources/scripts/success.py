# -*- coding: utf-8 -*-
"""
Created on Wed Jan 20 10:00:29 2021
@author: akombeiz
"""
#@VERSION=1.0
#@VIEWNAME=p21-success
#@MIMETYPE=zip
#@ID=success

import sys
import os


def verify_file(path):
    print(os.environ['username'])
    print(os.environ['password'])
    print(os.environ['connection-url'])
    print(os.environ['uuid'])
    print(os.environ['script_id'])
    print(os.environ['script_version'])
    print(os.environ['path_aktin_properties'])


def import_file(path):
    print(os.environ['username'])
    print(os.environ['password'])
    print(os.environ['connection-url'])
    print(os.environ['uuid'])
    print(os.environ['script_id'])
    print(os.environ['script_version'])
    print(os.environ['path_aktin_properties'])


if __name__ == '__main__':
    if len(sys.argv) != 3:
        raise SystemExit("sys.argv don't match")

    if sys.argv[1] == 'verify_file':
        verify_file(sys.argv[2])
    elif sys.argv[1] == 'import_file':
        import_file(sys.argv[2])
    else:
        raise SystemExit("unknown method function")
