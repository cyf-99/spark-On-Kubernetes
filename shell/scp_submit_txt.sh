#!/bin/bash

scp /home/cyf/flask_spark/data/submit.txt 192.168.10.201:/tmp

bak=`date -d yesterday +%Y%m%d`
mv /home/cyf/flask_spark/data/submit.txt /home/cyf/flask_spark/data/submit${bak}.txt
touch /home/cyf/flask_spark/data/submit.txt
