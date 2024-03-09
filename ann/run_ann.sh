#!/bin/bash

# run_ann.sh

cd /home/ec2-user/mpcs-cc/gas/ann
source /home/ec2-user/mpcs-cc/bin/activate
source /home/ec2-user/mpcs-cc/gas/web/.env
python /home/ec2-user/mpcs-cc/gas/ann/annotator.py
