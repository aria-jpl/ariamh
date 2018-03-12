#!/bin/bash

tarfiles=`ls *.gz`;

for tfile in $tarfiles
do
    echo "Untarring $tfile";
    `tar xzf $tfile`;
done

`rm -rf *.xml *.xsl *.gz`;
