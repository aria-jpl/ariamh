#!/usr/bin/env bash
ifgtagger=./ifgtagger.py
port=10000
hosts="$(hostname) localhost"
hostparm=""
for host in $hosts; do
    hostparm="--host $host:$port $hostparm"
done
cmd="bokeh serve --use-xheaders --port $port $hostparm $ifgtagger"
echo $cmd
$cmd
