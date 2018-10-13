#!/usr/bin/python

import sys

include = False
for line in sys.stdin:
  if line == "[ BEGIN STATS ]\n":
    include = True
    continue
  if line == "[ END STATS ]\n":
    include = False
    continue
  if include:
    print line.rstrip()
