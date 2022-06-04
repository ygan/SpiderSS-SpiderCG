#!/bin/bash

python generate_spiderSS.py --spiderSS_in_file Spider-SS/train_spider.json --preprocess_file train_spider-preprocessed.json --natsql_table NatSQLv1_6/tables_for_natsql.json --spiderSS_preprocessed_file  train_spider-SS-preprocessed.json  --spiderSS_for_models train_spider-SS-for-training.json
python generate_spiderSS.py --spiderSS_in_file Spider-SS/dev.json --preprocess_file dev-preprocessed.json --natsql_table NatSQLv1_6/tables_for_natsql.json --spiderSS_preprocessed_file  dev-SS-preprocessed.json  --spiderSS_for_models dev-SS-for-training.json


