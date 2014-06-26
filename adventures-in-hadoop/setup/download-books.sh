#!/bin/bash

bookIdsFile=$1
typeset -l titleString

cat ${bookIdsFile} | while read id title
do
    echo Downloading ${title}
    titleString=$(echo ${title} | sed 's/\s/-/g')
    curl --user-agent "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36" -o ${titleString}.txt http://www.gutenberg.org/files/${id}/${id}.txt
    sleep $(( $RANDOM % 55 + 5 ))
done
