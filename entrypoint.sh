#!/bin/sh

if [ "$1" != "" ]; then
 
  if [ "$1" = "--script" ]; then

    if [ $# -lt 2 ]; then
      echo "No script name"
      exit 1
    else
      SCRIPT=$2
      shift
      shift
    fi

  else
    SCRIPT=$1
    shift
  fi

  /usr/bin/env python /scripts/$SCRIPT "$@"

else
    echo "No script name"
    exit 1
fi

