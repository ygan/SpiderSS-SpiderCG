import json,argparse
from hashlib import md5
from natsql2sql.preprocess.sq import SubQuestion
from natsql2sql.preprocess.Schema_Token import Schema_Token
from natsql2sql.preprocess.TokenString import get_spacy_tokenizer
from natsql2sql.preprocess.sql_back import WHERE_OPS,AGG_OPS
from natsql2sql.natsql_parser import tokenize_nSQL,tokenize


def construct_hyper_param():
    parser = argparse.ArgumentParser()
    parser.add_argument('--spiderSS_in_file', default="", type=str)
    parser.add_argument('--preprocess_file', default='', type=str)
    parser.add_argument('--natsql_table', default='', type=str)
    parser.add_argument('--spiderSS_preprocessed_file', default='', type=str)
    parser.add_argument('--spiderSS_for_models', default='', type=str)
    args = parser.parse_args()
    return args


def check_data(args):
    sql_json = json.load(open(args.spiderSS_in_file, 'r'))
    if args.preprocess_file:
        sql_json2 = json.load(open(args.preprocess_file, 'r'))
        for (i,sql),sql2 in zip(enumerate(sql_json),sql_json2):
            if sql["question_md5"] != md5(sql2["question"].lower().encode('utf8')).hexdigest():
                print("The question ( "+sql2["question"]+" ) is different.")
            if sql["question_type"] != sql2["question_type"]:
                print("The split result of the question ( "+sql2["question"]+" ) is different.")
            sql["or_id"] = i
            sql["question"] = sql2["question"]
            sql["question_toks"] = sql2["question_toks"]
            sql["question_tag"] = sql2["question_tag"]
            sql["question_entt"] = sql2["question_entt"]
            sql["table_match"] = sql2["table_match"]
            sql["question_dep"] = sql2["question_dep"]
            sql["db_match"] = sql2["db_match"]
            sql["full_db_match"] = sql2["full_db_match"]
            sql["question_or"] = sql2["question_or"]
            sql["col_match"] = sql2["col_match"]
            sql["pattern_tok"] = sql2["pattern_tok"]
            sql["question_lemma"] = sql2["question_lemma"]
        json.dump(sql_json,open(args.spiderSS_preprocessed_file,"w"),indent=2)
    return sql_json


def generate_split_data(args,sql_json):
    remove_miss_select_and_groupby = True
    allow_single_miss = False
    tables = json.load(open(args.natsql_table, 'r'))
    all_tables = {}
    all_schema = {}
    for t in tables:
        all_tables[t['db_id']] = t
    
    sql_json_sp = []
    for sql in sql_json:
        sq = SubQuestion(sql["question"],sql["question_type"],sql["table_match"],sql["question_tag"],{"root":0,"data":[""]*len(sql["table_match"])},sql["question_entt"],sql,run_special_replace=False)
        if sql["db_id"] not in all_schema:
            all_schema[sql["db_id"]] = Schema_Token(get_spacy_tokenizer(), None, all_tables[sql["db_id"]], None)
            all_schema[sql["db_id"]].add_lower_data(all_tables[sql["db_id"]])
        schema = all_schema[sql["db_id"]]
        sql_sp = []

        for top_select in sql['sql']['select'][1]:
            sql_sp.append(["select",top_select])

        if sql['sql']['where']:
            for w in sql['sql']['where']:
                sql_sp.append(["where",w])
        
        if sql['sql']['orderBy']:
            sql_sp.append(["orderBy",sql['sql']['orderBy']])

        if sql['sql']['limit']:
            sql_sp.append(["limit",sql['sql']['limit']])

        if sql['sql']['union']:
            sql_sp.append(["union",sql['sql']['union']['select'][1][0]])

            
        if sql['sql']['intersect']:
            sql_sp.append(["intersect",sql['sql']['intersect']['select'][1][0]])

        if sql['sql']['groupBy']:
            sql_sp.append(["groupBy",sql['sql']['groupBy']])
        
        sql_sp.append(["NONE",None])
        label_group_by = False
        label_select = False
        if 'match_link' not in sql:
            sql['match_link'] = [[-1]] * len(sq.sub_sequence_type)
        for (i,ssl),sst,ml in zip(enumerate(sq.sub_sequence_list),sq.sub_sequence_type,sql['match_link']):
            new_sql = {}
            if i == len(sq.sub_sequence_list)-1:
                new_sql["or_data"] = sql
            new_sql["db_id"] = sql["db_id"]
            new_sql["or_id"] = sql["or_id"]
            new_sql["sp_id"] = i
            new_sql["sp_num"] = len(sq.sub_sequence_list)
            new_sql["question_range"] = [sq.original_idx[i][0],sq.original_idx[i][-1]]
            new_sql["question"] = sql["question"]
            
            new_sql["question_toks"] = new_sql["question"].split(" ")
            new_sql["sub_question"] = " ".join([ new_sql["question_toks"][c] for c in range(sq.original_idx[i][0],sq.original_idx[i][-1]+1)])
            
            new_sql["sql"] = {
                'db_id': sql['db_id'],
                'limit':None,
                'intersect':None,
                'union':None,
                'except':None,
                'having':[],
                'orderBy':[],
                'groupBy':[],
                'where':[],
                "select":[True,[]] if sql['sql']['select'][0] else [False,[]],
                'extra':None,
                # 'bcol':None
            }
            skip_times = 0
            
            for ml_idx,sql_idx in enumerate(ml):
                if skip_times > 0:
                    skip_times -= 1
                    continue
                if sql_idx == -1:
                    continue
                elif type(sql_idx) == str:
                    sql_str = sql_idx.lower().strip()
                    if sql_str.startswith("select "):
                        toks = tokenize(sql_str)
                        assert len(toks) == 2 or len(toks) == 5
                        agg_idx = 0 if len(toks) == 2 else AGG_OPS.index(toks[1])
                        col_token = toks[1] if len(toks) == 2 else toks[3]
                        col_idx = schema.table_column_names_original_low.index(col_token) if "." in col_token else '@'
                        new_sql["sql"]["select"][1].append([agg_idx, [0, [0,col_idx,False], None]])
                    elif sql_str.startswith("group "):
                        toks = tokenize(sql_str)
                        assert len(toks) == 3
                        col_idx = schema.table_column_names_original_low.index(toks[2]) if "." in toks[2] else '@'
                        new_sql["sql"]['groupBy'] = [[0, col_idx, False]]
                        label_group_by = True
                    elif sql_str.startswith("order "):
                        toks = tokenize(sql_str)
                        if toks[-2] == "limit":
                            new_sql["sql"]["limit"] = int(toks[-1])
                            toks = toks[:-2]
                        if toks[-1] != "desc" and  toks[-1] != "asc":
                            toks.append("asc")
                        assert len(toks) == 7 or len(toks) == 4
                        agg_idx = 0 if len(toks) == 4 else AGG_OPS.index(toks[2])
                        col_token = toks[2] if len(toks) == 4 else toks[4]
                        col_idx = schema.table_column_names_original_low.index(col_token) if "." in col_token else '@'
                        new_sql["sql"]["orderBy"] = [toks[-1], [[0, [agg_idx,col_idx,False], None]]]
                    elif sql_str.startswith("extra "):
                        toks = tokenize_nSQL(sql_str.lower(), None, sepearte_star_name = False)
                        assert len(toks) == 2 or len(toks) == 5
                        agg_idx = 0 if len(toks) == 2 else AGG_OPS.index(toks[1])
                        col_token = toks[1] if len(toks) == 2 else toks[3]
                        col_idx = schema.table_column_names_original_low.index(col_token) if "." in col_token and col_token != "@.@" else '@'
                        new_sql["sql"]['extra'] = [agg_idx, col_idx, False]
                    # elif sql_str.startswith("bcol "):
                    #     toks = tokenize_nSQL(sql_str.lower(), None, sepearte_star_name = False)
                    #     assert len(toks) == 2 
                    #     agg_idx = 0 if len(toks) == 2 else AGG_OPS.index(toks[1])
                    #     col_token = toks[1] if len(toks) == 2 else toks[3]
                    #     col_idx = schema.table_column_names_original_low.index(col_token) if "." in col_token and col_token != "@.@" else '@'
                    #     new_sql["sql"]['bcol'] = [agg_idx, col_idx, False]
                    elif sql_str.startswith("where ") and not new_sql["sql"]['where']:
                        toks = tokenize_nSQL(sql_str.lower(), None, sepearte_star_name = False)
                        assert len(toks) in [4,5,7]
                        agg_off = 0 if len(toks) in [4,5] else 3
                        col_token = toks[1] if len(toks) in [4,5] else toks[3]
                        where_not = True if toks[2+agg_off] == 'not' else False
                        agg_idx = AGG_OPS.index(toks[1]) if len(toks) == 7 else 0
                        col_idx = schema.table_column_names_original_low.index(col_token) if "." in col_token and col_token != "@.@" else '@'
                        opt = toks[3+agg_off] if where_not else toks[2+agg_off]
                        opt = WHERE_OPS.index(opt)
                        val = toks[-1]
                        if val in schema.table_column_names_original_low:
                            val = [0, schema.table_column_names_original_low.index(val), False]

                        # if natsql_version() == "1.7":
                        #     if where_not and opt == 12:
                        #         opt = 8
                        #         where_not = True
                        #     elif where_not and opt == 14:
                        #         opt = 1
                        #         where_not = True
                        #     elif where_not and opt == 13:
                        #         opt = 9
                        #         where_not = True
                        #     assert False
                        # else:
                        if where_not and opt == 8:
                            opt = 12
                        elif where_not and opt == 1:
                            opt = 14
                        elif where_not and opt == 9:
                            opt = 13
                        new_sql["sql"]["where"] = [[False, opt, [0, [agg_idx, col_idx, False], None], val, None]]
                    else:
                        assert False
                else:
                    miss_col = True if ml_idx + 1 < len(ml) and ml[ml_idx + 1] == -1 else False
                    if sql_sp[sql_idx][0] == "NONE":
                        num_of_num = sum([0 if type(mm) == str or mm == -1 else 1 for mm in ml])
                        if num_of_num != 1:
                            print(sql["or_id"])
                        assert num_of_num == 1
                    elif sql_sp[sql_idx][0] == "select":
                        if miss_col:
                            if remove_miss_select_and_groupby and ml_idx + 2 < len(ml) and type(ml[ml_idx + 2]) == str and ml[ml_idx + 2].strip().lower().replace("  "," ") == "group by @":
                                skip_times = 2
                                continue
                            elif remove_miss_select_and_groupby and ml_idx + 3 < len(ml) and ml[ml_idx + 3] == -1 and sql_sp[ml[ml_idx + 2]][0] == 'groupBy':
                                skip_times = 3
                                if len(ml) != 4:
                                    print("check ml is not 4:"+str(sql["or_id"]))
                                continue
                            elif remove_miss_select_and_groupby and ml.count(-1) >= 2:
                                print("check two -1:"+str(sql["or_id"]))
                            if allow_single_miss:
                                new_sql["sql"]["select"][1].append([0, [0, [0,'@',False], None]])
                            else:
                                new_sql["sql"]["select"][1].append(sql_sp[sql_idx][1])
                        else:
                            new_sql["sql"]["select"][1].append(sql_sp[sql_idx][1])
                    elif sql_sp[sql_idx][0] == "where":
                        if miss_col and type(sql_sp[sql_idx][1]) == list and allow_single_miss:
                            new_sql["sql"]["where"].extend([sql_sp[sql_idx][1][0], sql_sp[sql_idx][1][1], [0, [0, '@', False], None], sql_sp[sql_idx][1][3], sql_sp[sql_idx][1][4]])
                        else:
                            new_sql["sql"]["where"].append(sql_sp[sql_idx][1])
                    else:
                        if miss_col and sql_sp[sql_idx][0] == 'orderBy' and allow_single_miss:
                            new_sql["sql"][sql_sp[sql_idx][0]] = [sql_sp[sql_idx][1][0], [[0, [0,'@',False], None]]]
                            if sql_sp[sql_idx][1][1][0][1][0] == 3:
                                new_sql["sql"][sql_sp[sql_idx][0]][1][0][1][0] = 3
                        elif miss_col and sql_sp[sql_idx][0] == 'groupBy':
                            if remove_miss_select_and_groupby and ml_idx + 2 < len(ml) and type(ml[ml_idx + 2]) == str and ml[ml_idx + 2].strip().lower().replace("  "," ") == "select @":
                                skip_times = 2
                                assert len(ml) == 3
                                continue
                            if allow_single_miss:
                                new_sql["sql"][sql_sp[sql_idx][0]] = [[0, '@', False]]
                            else:
                                new_sql["sql"][sql_sp[sql_idx][0]] = sql_sp[sql_idx][1]
                                label_group_by = True
                        elif miss_col and allow_single_miss:
                            assert False
                        else:
                            if sql_sp[sql_idx][0] == 'groupBy':
                                label_group_by = True
                            new_sql["sql"][sql_sp[sql_idx][0]] = sql_sp[sql_idx][1]
            if label_select and new_sql["sql"]["select"][1] and not new_sql["sql"]["groupBy"]:
                pass
            if new_sql["sql"]["select"][1] and not new_sql["sql"]["groupBy"]:
                label_select = True
            if new_sql["sql"]["where"]:
                if new_sql["sql"]["where"][0] in ["and","or"]:
                    del new_sql["sql"]["where"][0]
                if not new_sql["sql"]["where"]:
                    print(sql["or_id"])
                assert new_sql["sql"]["where"]
                if new_sql["sql"]["where"][-1] in ["and","or"]:
                    del new_sql["sql"]["where"][-1]
                if type(new_sql["sql"]["where"][-1]) == str:
                    print(sql["or_id"])
            if not new_sql["sql"]["select"][1]:
                new_sql["sql"]["select"][0] = False
            sql_json_sp.append(new_sql)
        if not label_group_by and sql['sql']['groupBy']:
            pass
    json.dump(sql_json_sp,open(args.spiderSS_for_models,'w'), indent=2)



if __name__ == '__main__':
    args = construct_hyper_param()
    sql_json = check_data(args)
    generate_split_data(args,sql_json)