#!/bin/bash

python setence_split.py --in_file NatSQLv1_6/dev.json  --out_file dev-ss.json   --keep_or
python pattern_generation.py --in_file dev-ss.json  --out_file dev-preprocessed.json  --keep_or
rm dev-ss.json 

python setence_split.py --in_file NatSQLv1_6/train_spider.json  --out_file train_spider-ss.json   --keep_or
python pattern_generation.py --in_file train_spider-ss.json  --out_file train_spider-preprocessed.json  --keep_or
rm train_spider-ss.json 

