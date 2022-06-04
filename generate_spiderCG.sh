#!/bin/bash
echo "Start"
python generate_spiderCG.py --spiderSS_preprocessed_file  train_spider-SS-preprocessed.json --spiderSS_for_models train_spider-SS-for-training.json --database data/database --natsql_table NatSQLv1_6/tables_for_natsql.json --orgin_table NatSQLv1_6/tables.json --CG_type substitute --spiderCG_out_file train_spider-CG_SUB.json 1>train_spider-CG_SUB.txt


python generate_spiderCG.py --spiderSS_preprocessed_file  train_spider-SS-preprocessed.json --spiderSS_for_models train_spider-SS-for-training.json --database data/database --natsql_table NatSQLv1_6/tables_for_natsql.json --orgin_table NatSQLv1_6/tables.json --CG_type append --spiderCG_out_file train_spider-CG_APP.json 1>train_spider-CG_APP.txt


python generate_spiderCG.py --spiderSS_preprocessed_file  dev-SS-preprocessed.json --spiderSS_for_models dev-SS-for-training.json --database data/database --natsql_table NatSQLv1_6/tables_for_natsql.json --orgin_table NatSQLv1_6/tables.json --CG_type substitute --spiderCG_out_file dev-CG_SUB.json 1>dev-CG_SUB.txt


python generate_spiderCG.py --spiderSS_preprocessed_file  dev-SS-preprocessed.json --spiderSS_for_models dev-SS-for-training.json --database data/database --natsql_table NatSQLv1_6/tables_for_natsql.json --orgin_table NatSQLv1_6/tables.json --CG_type append --spiderCG_out_file dev-CG_APP.json 1>dev-CG_APP.txt
echo "Finish"