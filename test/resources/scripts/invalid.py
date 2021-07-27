# -*- coding: utf-8 -*-
"""
Created on Wed Jan 20 10:00:29 2021
@author: akombeiz
"""
#@VERSION=1.0
#@VIEWNAME=p21-error
#@MIMETYPE=zip

import sys


def verify_file(path):
    print("verifying " + path2)


def import_file(path):
    print("importing " + path2)


if __name__ == '__main__':
    if len(sys.argv) != 3:
        raise SystemExit("sys.argv don't match")

    if sys.argv[1] == 'verify_file':
        verify_file(sys.argv[2])
    elif sys.argv[1] == 'import_file':
        import_file(sys.argv[2])
    else:
        raise SystemExit("unknown method function")
