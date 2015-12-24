#!/bin/bash

for h in lab{1..5}
do
  echo $h
  ssh $h w
done 2> /dev/null # throw away the warning message at login.
