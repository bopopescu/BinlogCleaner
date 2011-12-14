#!/bin/bash

pid=`ps -ef | fgrep "cleaner.py" |fgrep -v "fgrep" | awk '{print $2}'`
echo $pid

if [ $pid ]
then
	kill -9 $pid 
fi
