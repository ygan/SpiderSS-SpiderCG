# SpiderSS-SpiderCG

## Introduction
This repository is built upon the [NatSQL](https:/www.github.com/ygan/NatSQL). Some algorithms mentioned in the SpiderSS paper are stored in the NatSQL repository, such as the sentence split. You should download the NatSQL and this repository, then combine these two repositories by copying the files in this repository into the root path of the NatSQL.

## Environment Setup
After combination, install Python dependency via `pip install -r requirements.txt`. 



## Usage

### Step 1: Download the Spider dataset

Download the datasets: [Spider](https://yale-lily.github.io/spider). Make sure to download the `06/07/2020` version or newer.
Unpack the datasets somewhere outside this project and put `train_spider.json`, `dev.json`,  `tables.json` and `database` folder under `./data/` directory.

Run `check_and_preprocess.sh` to check and preprocess the dataset. It will generate (1) the `train_spider.json` and `dev.json` with NatSQL<sub>G</sub> ; (2) preprocessed `tables.json` and `tables_for_natsql.json` ; under  `./NatSQLv1_6/` directory. 

### Step 2: Preprocess the Spider dataset

Run `sh preprocess_spider.sh` to preprocess the Spider dataset.
You should get preprocess files `train_spider-preprocessed.json` and `dev-preprocessed.json`. Alternatively, You can download our preprocessed Spider dataset [here](https://drive.google.com/drive/folders/1LLrPJE6r9HKjcC1-1D77aTSLHoOVGLUO?usp=sharing).



### Step 3: Generate the SpiderSS dataset
Run `sh generate_spiderSS.sh` to generate the SpiderSS dataset.
You should get spiderSS files `train_spider-SS-preprocessed.json`, `train_spider-SS-for-training.json`, `dev-SS-preprocessed.json`, and `dev-SS-for-training.json` . The two `*-for-training` files can be used by modified models. Alternatively, You can download our generated Spider-SS dataset [here](https://drive.google.com/drive/folders/17wZ5c6epXfSVzZ_Le1RsXc_WSvtVWPWD?usp=sharing).


### Step 4: Generate the SpiderCG dataset
Run `sh generate_spiderCG.sh` to generate the SpiderCG dataset.
You should get spiderCG files `train_spider-CG_SUB.json`, `train_spider-CG_APP.json`, `dev-CG_SUB.json`, and `dev-CG_APP.json`. Alternatively, You can download our generated Spider-CG dataset [here](https://drive.google.com/drive/folders/1XE3itFwQUmUHgbbkd1sLlR8wnXXvzj7U?usp=sharing).


## License
The code and data are under the [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/legalcode) license.