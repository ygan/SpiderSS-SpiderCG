import json,os,copy,itertools

from natsql2sql.natsql_parser import create_sql_from_natSQL
from natsql2sql.natsql2sql import get_num_for_limit,reversed_link_back_col,Args
from natsql2sql.natsql2sql import analyse_num,no_more_num_cond,search_db,search_bcol
from natsql2sql.preprocess.TokenString import get_spacy_tokenizer
from natsql2sql.preprocess.sq import SubQuestion
from natsql2sql.preprocess.sql_back import sql_back
from natsql2sql.preprocess.stemmer import MyStemmer 
from natsql2sql.utils import str_is_num, str2num


def next_beam(beam_idx,type_,beam_pool):
    next_ = []
    type_contained_idx = []
    beam_list = []
    for (i,b_i), sql in zip(enumerate(beam_idx),beam_pool):
        if type_ == "select" and sql[b_i]["select"][1]:
            type_contained_idx.append(i)
        elif type_ in ["where","orderBy"] and sql[b_i][type_]:
            type_contained_idx.append(i)
        elif type_ == "all":
            type_contained_idx.append(i)
        beam_list.extend(range(len(beam_pool[i])))

    beam_list = list(set([i for i in itertools.combinations(beam_list,len(beam_pool))]))
    beam_list.sort()
    real_beam_list = []
    last_score = sum([ob[beam_idx[j]]['score'] for j,ob in enumerate(beam_pool)])
    for bl in beam_list:
        if sum(bl) == 0:
            continue
        elif bl == beam_idx:
            continue
        skip = False
        for (i,b_i) in enumerate(bl):
            if (i not in type_contained_idx and b_i != beam_idx[i]) or b_i >= len(beam_pool[i]):
                skip = True
        if skip:
            continue
        else:
            score = sum([ob[bl[j]]['score'] for j,ob in enumerate(beam_pool)])
            if score < last_score:
                real_beam_list.append([score,bl])
    real_beam_list.sort(key = lambda x:-x[0])
    for bl in real_beam_list:
        yield (bl[0], bl[1], [b[bl[1][i]] for i,b in enumerate(beam_pool)])


def check_select(p_nsql,find_table):
    if len(p_nsql['select'][1]) > 1:
        for i in range(len(p_nsql['select'][1])-1):
            for j in range(i+1,len(p_nsql['select'][1])):
                if p_nsql['select'][1][i][1][1][1] == p_nsql['select'][1][j][1][1][1] and ((not p_nsql['select'][1][i][0] and not p_nsql['select'][1][i][1][1][1]) or (not p_nsql['select'][1][j][0] and not p_nsql['select'][1][j][1][1][1])):
                    return False
    for select in p_nsql['select'][1]:
        if  select[0] in [1,2,4,5] or select[1][1][0] in [1,2,4,5]:
           if find_table['column_types_checked'][select[1][1][1]] in ["text","None","others"]:
                return False
        elif find_table['column_names_original'][select[1][1][1]][1] == "*" and len(p_nsql['select'][1]) > 1 and  select[0] == 0 and select[1][1][0] == 0:
            return False
        if find_table['column_names'][select[1][1][1]][1] == "*" and not select[0] and not select[1][1][0] and len(p_nsql['select'][1]) > 1:
            return False
        elif find_table['column_names'][select[1][1][1]][1] == "*" and (select[0] not in [0,3] or select[1][1][0] not in [0,3]):
            return False
    return True

def check_where(p_nsql,schema_ts):
    if len(p_nsql["where"]) > 2 :
        for i in range(1,len(p_nsql["where"])):
            if type(p_nsql["where"][i]) == type(p_nsql["where"][i-1]):
                return False
    for where in p_nsql["where"]:
        if type(where) == list:
            if type(where[2][1][1]) == int and schema_ts.column_names_original[where[2][1][1]][1] == "*" and where[2][1][0] != 3 and type(where[3]) != list:
                return False
    if len(p_nsql["where"]) == 3 and p_nsql['where'][0][2][1][1] == p_nsql['where'][2][2][1][1] and p_nsql['where'][0][1] == p_nsql['where'][2][1] and (type(p_nsql['where'][0][2][1][1]) == str or (schema_ts.column_types[p_nsql['where'][0][2][1][1]] == "boolean" and p_nsql['where'][0][3] == p_nsql['where'][2][3])):
        return False
    if len(p_nsql["where"]) == 3 and type(p_nsql['where'][0]) == list and type(p_nsql['where'][2]) == list and p_nsql['where'][0][2][1][1] == p_nsql['where'][2][2][1][1]:
        if p_nsql['where'][0][1] != p_nsql['where'][2][1] and p_nsql['where'][0][1] in [2,9] and p_nsql['where'][2][1] in [2,9]:
            return False
    if len(p_nsql["where"]) == 1 and p_nsql['where'][0][1] == 3 and type(p_nsql['where'][0][3]) == list and p_nsql['where'][0][3][0] == 1 and not p_nsql['orderBy']:
        return False
    if len(p_nsql["where"]) == 1 and p_nsql['where'][0][1] == 4 and type(p_nsql['where'][0][3]) == list and p_nsql['where'][0][3][0] == 2 and not p_nsql['orderBy']:
        return False
    if len(p_nsql["where"]) >= 1 and type(p_nsql['where'][-1]) == list and type(p_nsql['where'][-1][3]) == list and p_nsql['where'][-1][1] in [2,8] and p_nsql['where'][-1][3][1] == p_nsql['where'][-1][2][1][1] and not p_nsql['where'][-1][2][1][0] and not p_nsql['where'][-1][3][0]:
        return False
    if len(p_nsql["where"]) == 2 and type(p_nsql['where'][0]) == str and type(p_nsql['where'][1]) == list and type(p_nsql['where'][1][3]) == list:
        t_id = schema_ts.column_tokens_table_idx[p_nsql['where'][1][3][1]]
        for select in p_nsql['select'][1]:
            if t_id == schema_ts.column_tokens_table_idx[select[1][1][1]]:
                return False
        for where in p_nsql["where"]:
            if type(where) == list:
                if where[1] in [3,4,5,6] and where[2][1][0] != 3 and type(where[2][1][1]) == int:
                    if find_table['column_types_checked'][where[2][1][1]] in ["text","boolean"]:
                        return False
    return True


def check_orderBy(p_nsql,find_table):
    if p_nsql['orderBy']:
        for col in p_nsql['orderBy'][1]:
            if col[1][0] in [1,2,4,5] and find_table['column_types_checked'][col[1][1]] == "text":
                return False
            if find_table['column_names'][col[1][1]][1] == "*" and col[1][0] != 3:
                return False            
    return True

def agg_conflict(all_sql,col,schema):
    for idx,sql in enumerate(all_sql):
        if sql['select'][1]:
            for sel in sql['select'][1]:
                if (sel[1][1][0] or sel[0]) and schema.column_tokens_table_idx[col] == schema.column_tokens_table_idx[sel[1][1][1]]:
                    return True
        for w in sql['where']:
            if type(w) == list and w[2][1][0] and schema.column_tokens_table_idx[col] == schema.column_tokens_table_idx[w[2][1][1]]:
                return True
        if sql['orderBy'] and sql['orderBy'][1][0][1][0] and schema.column_tokens_table_idx[col] == schema.column_tokens_table_idx[sql['orderBy'][1][0][1][1]]:
            return True
    return False

def col_in_select_tables(col_id,select,schema):
    table_id = schema.column_names_original[col_id][0]
    for select in select[1]:
        if schema.column_names_original[select[1][1][1]][0] == table_id:
            return True
    return False
def  col_in_where_tables(col_id,wheres,schema):
    table_id = schema.column_names_original[col_id][0]
    for w in wheres:
        if type(w) == list:
            if type(w[3]) == list or type(w[2][1][1]) != int:
                break
            if schema.column_names_original[w[2][1][1]][0] == table_id:
                return True
    return False
def generate_right_col_for_where(col_id,wheres,schema):
    table_id = schema.column_names_original[col_id][0]
    for i,w in enumerate(wheres):
        if type(w) == list:
            if type(w[3]) == list or type(w[2][1][1]) != int:
                break
            table_where = schema.column_names_original[w[2][1][1]][0]
            if table_where == table_id:
                return i,col_id
            for nt in schema.original_table['network']:
                if len(nt[1]) == 2 and table_where in nt[1] and table_id in nt[1]:
                    if nt[0][0][0] == col_id and schema.column_names_original[nt[0][0][1]][0] == table_where:
                        return i,nt[0][0][1]
                    if nt[0][0][1] == col_id and schema.column_names_original[nt[0][0][0]][0] == table_where:
                        return i,nt[0][0][0]
    return -1,0
def generate_right_col_for_orderby(col_id,order,schema):
    table_id = schema.column_names_original[col_id][0]
    for i,o in enumerate(order[1]):
        if type(o[1][1]) != int:
            break
        table_order = schema.column_names_original[o[1][1]][0]
        for nt in schema.original_table['network']:
            if len(nt[1]) == 2 and table_order in nt[1] and table_id in nt[1]:
                if nt[0][0][0] == col_id:
                    return i,nt[0][0][1]
                if nt[0][0][1] == col_id:
                    return i,nt[0][0][0]
    return -1,0


def generate_natsql_from_split_data(one_full_sql,sq,schema,beam_idsx, database, fill_value = True):
    
    def add_select(one_full_sql,new_sql,schema,sq,used_value,fill_value):
        def allow_adding(all_sql,sql_idx,sel_in,select_store,schema):
            def ignore_groupBy_select(col_g_select,selects,schema):
                for sel_tmp in selects:
                    if schema.column_tokens_table_idx[sel_tmp[1][1][1]] != schema.column_tokens_table_idx[col_g_select]:
                        continue
                    if schema.column_tokens_lemma_str[sel_tmp[1][1][1]] == "name" or schema.column_tokens_lemma_str[sel_tmp[1][1][1]] == "title"  or schema.column_tokens_lemma_str[sel_tmp[1][1][1]].endswith(" name"): 
                        return True
                    if not sel_tmp[0] and not sel_tmp[1][1][0]:
                        pass
                    else:
                        if schema.column_tokens_table_idx[col_g_select] == schema.column_tokens_table_idx[sel_tmp[1][1][1]] and col_g_select in schema.primaryKey:
                            return True
                return False

            if sel in select or sel[1][1][1] == "@":
                return False
            if all_sql[sql_idx]['groupBy'] and len(all_sql[sql_idx]['select'][1]) <= 2 and all_sql[sql_idx]['groupBy'][0][1] == sel[1][1][1] and not sel[1][1][0] and not sel[0]:
                col = sel[1][1][1]
                if col in schema.primaryKey or col in schema.foreignKey:
                    # there is name in other sub question:
                    for i in range(len(all_sql)):
                        if i != sql_idx and ignore_groupBy_select(col,all_sql[i]["select"][1],schema):
                            return False
                            
                                 
                elif schema.column_tokens_lemma_str[col] == "name" or schema.column_tokens_lemma_str[col] == "title" or schema.column_tokens_lemma_str[col].endswith(" name"):
                    # there is id in other sub question:
                    for i in range(len(all_sql)):
                        if i != sql_idx:
                            for sel_tmp in all_sql[i]["select"][1]:
                                if sel_tmp[1][1][1] in schema.primaryKey:
                                    return False
                for i in range(len(all_sql)):
                    if i != sql_idx and all_sql[i]["groupBy"] and all_sql[i]["select"][1] and col == all_sql[i]["groupBy"][0][1] and True not in [True if sel[1][1][1] == col else False for sel in all_sql[i]["select"][1]]: 
                        return False
                    elif True in [True if (type(sel[1][1][1]) == int and schema.column_tokens_lemma_str[sel[1][1][1]] == "*" and not sel[0] and not sel[1][1][0]) else False for sel in all_sql[i]["select"][1]]:
                        return False
                if col in schema.primaryKey and agg_conflict(all_sql,col,schema):
                    return False
            return True
        select = []
        for idx,sql in enumerate(one_full_sql):
            if sql['select'][0]:
                new_sql['select'][0] = True
            for sel in sql['select'][1]:
                if allow_adding(one_full_sql,idx,sel,select,schema):
                    select.append(sel)

            if sql['extra'] and idx > 0 and len(one_full_sql[idx-1]['select'][1]) == 1 and not sql['where'] and not sql['orderBy']:
                if one_full_sql[idx-1]['select'][1][0][1][1][1] == sql['extra'][1]:
                    sql['extra'] = None
                elif one_full_sql[idx-1]['select'][1][0][1][1][1] == "@":
                    one_full_sql[idx-1]['select'][1][0][1][1][1] = sql['extra'][1]
                    sel = one_full_sql[idx-1]['select'][1][0]
                    if allow_adding(one_full_sql,idx-1,sel,select,schema):
                        select.append(sel)
                
        if len(select) == 1 and len(one_full_sql) > 1:
            for idx,sql in enumerate(one_full_sql):
                for sel in sql['select'][1]:
                    if sel[1][1][1] == "@" and (sel[1][1][0] or sel[0]):
                        select[0][1][1][0],select[0][0] = sel[1][1][0] , sel[0]
        new_sql["select"][1] = select
        if check_select(new_sql,schema.original_table):
            return True
        return False

    def add_groupBy(one_full_sql,new_sql,schema):
        groupBy = []
        for idx,sql in enumerate(one_full_sql):
            if sql['groupBy']:
                groupBy.append(sql['groupBy'])
                if sql['groupBy'][0][1] == "@":
                    print('sql[groupBy][0][1] == @')
        if len(groupBy) <= 1:
            new_sql["groupBy"] = groupBy[0] if groupBy else groupBy
        elif len(groupBy) == 2 and groupBy[0] == groupBy[1]:
            new_sql["groupBy"] = groupBy[0]
        else:
            for idx,sql in enumerate(one_full_sql):
                if sql['groupBy'] and sql["where"]:
                    new_sql["groupBy"] = sql['groupBy']
                    return
                elif sql['groupBy'] and len(sql["select"][1]) > 1:
                    new_sql["groupBy"] = sql['groupBy']
                    return
            for g in groupBy:
                if g[0][1] not in schema.primaryKey:
                    new_sql["groupBy"] = g
                    return

    def remove_groupBY(one_full_sql,new_sql,schema):
        if not new_sql["groupBy"]:
            return 
        if schema.column_names_original[new_sql['groupBy'][0][1]][1] == "*":
            new_sql["groupBy"] = []
            return
        sel = [True if (sel[1][1][0] or sel[0]) else False for sel in new_sql['select'][1]]
        if True in sel and False in sel:
            return 

        sub_query = False
        for w in new_sql['where']:
            if type(w) == list and w[2][1][0]:
                return
            if type(w) == list and type(w[3]) == list:
                sub_query = True
                break
        if sub_query or not new_sql["orderBy"] or not new_sql["orderBy"][1][0][1][0]:
            # remove Group BY and its Select
            g_col = new_sql["groupBy"][0][1]
            if len(new_sql['select'][1]) > 1 and True in [True if not sel[1][1][0] and not sel[0] and g_col == sel[1][1][1] else False for sel in new_sql['select'][1]]:
                keep_the_select = False
                for sql in one_full_sql:
                    if not sql["groupBy"] and sql['select'][1]:
                        for sel in sql['select'][1]:
                            if not sel[1][1][0] and not sel[0] and g_col == sel[1][1][1]:
                                keep_the_select = True
                                break
                if not keep_the_select and not new_sql["where"] and not new_sql["orderBy"] and True not in [True if sel[1][1][1] != g_col and schema.column_tokens_table_idx[g_col] == schema.column_tokens_table_idx[sel[1][1][1]] else False for sel in new_sql['select'][1] ]:
                    keep_the_select = True
                if not keep_the_select:
                    for i,sel in enumerate(new_sql['select'][1]):
                        if not sel[1][1][0] and not sel[0] and g_col == sel[1][1][1]:
                            del(new_sql['select'][1][i])
                            break
            elif False not in sel and len(new_sql['groupBy']) == 1 and not new_sql['groupBy'][0][0]:
                keep_group = True
                for sql in one_full_sql:
                    if sql['groupBy'] and (sql['orderBy'] or sql['where'] or sql['limit'] or sql['intersect'] or sql['union'] or sql['extra']):
                        keep_group = False
                if keep_group:
                    new_sql['select'][1].append([0, [0, new_sql['groupBy'][0], None]])
                    return

            new_sql["groupBy"] = []

        return
        

    def add_orderBy(one_full_sql,new_sql,schema,sq,used_value,fill_value):
        limit = []
        for (idx,sql), pts in zip(enumerate(one_full_sql),sq.pattern_tok):
            if sql['limit']:
                if fill_value and "NUM" in pts and ("GR_JJS" in pts or "SM_JJS" in pts or "top" in pts):
                    num_idx,num = get_num_for_limit(pts,sq,sql,idx,schema.original_table)
                    if num and type(num) == int and [idx,num_idx] not in used_value:
                        sql['limit'] = num
                        used_value.append([idx,num_idx])
                limit.append(sql['limit'])
                
                if sql['orderBy'] and idx > 0 and one_full_sql[idx-1]['extra'] and not sql['extra'] and not one_full_sql[idx-1]['where'] and not one_full_sql[idx-1]['orderBy'] and one_full_sql[idx-1]['extra'][0] == 0 and (one_full_sql[idx-1]['extra'][1] in schema.foreignKey or one_full_sql[idx-1]['extra'][1] in schema.primaryKey) :
                    sql['extra'] = one_full_sql[idx-1]['extra']
                    one_full_sql[idx-1]['extra'] = None
                elif sql['orderBy'] and idx > 0 and one_full_sql[idx-1]['extra'] and sql['orderBy'][1] and (sql['orderBy'][1][0][1][1] == one_full_sql[idx-1]['extra'][1] or type(sql['orderBy'][1][0][1][1]) != int):
                    sql['orderBy'][1][0][1][1] = one_full_sql[idx-1]['extra'][1]
                    one_full_sql[idx-1]['extra'] = None
                if sql['orderBy'] and not new_sql['where'] and sql['extra'] and sql['extra'][0] == 0 and (sql['extra'][1] in schema.foreignKey or sql['extra'][1] in schema.primaryKey):
                    new_where = [False, 8, [0, [0,'@.@',False], None], [0,0,False], None]
                    if col_in_select_tables(sql['extra'][1],new_sql['select'],schema):
                        new_where[2][1] = sql['extra']
                        insert_idx,col_right = generate_right_col_for_orderby(sql['extra'][1],sql['orderBy'],schema)
                        if insert_idx >= 0:
                            new_where[3][1] = col_right
                            new_sql['where'] = [new_where]
                            one_full_sql[idx]['extra'] = None
                    else:
                        new_where[3] = sql['extra']
                        new_sql['where'] = [new_where]
                        one_full_sql[idx]['extra'] = None

        if len(limit) > 0:
            max_l = 1
            for l in limit:
                if l > max_l:
                    max_l = l
            new_sql["limit"] = max_l

        orderBy = []
        orderBy_idx = 0
        for idx,sql in enumerate(one_full_sql):
            if sql['orderBy']:
                orderBy.append(sql['orderBy'])
                orderBy_idx = idx
        if len(orderBy) <= 1:
            new_sql["orderBy"] = orderBy[0] if orderBy else orderBy
        elif len(orderBy) == 2 and orderBy[0] == orderBy[1]:
            new_sql["orderBy"] = orderBy[0]
        else:
            new_sql["orderBy"] = orderBy[0]
            for orb in orderBy:
                if orb[0] == 'desc':
                    new_sql["orderBy"][0] = orb[0]
                if len(orb[1]) > 1:
                    new_sql["orderBy"][1] = orb[1]
                elif orb[1][0][1][1] != '@':
                    new_sql["orderBy"][1] = orb[1]
        if new_sql["orderBy"] and new_sql["orderBy"][1][0][1][1] == '@':
            if len(new_sql['select'][1]) == 1:
                new_sql["orderBy"][1][0] = new_sql['select'][1][0][1] if new_sql['orderBy'][1][0][1][0] == 0 else [new_sql['orderBy'][1][0][0], [new_sql['orderBy'][1][0][1][0], schema.tbl_col_idx_back[schema.column_tokens_table_idx[new_sql['select'][1][0][1][1][1]]][0], new_sql['orderBy'][1][0][1][2]], new_sql['orderBy'][1][0][2]]
                if not new_sql['orderBy'][1][0][1][0] and new_sql['select'][1][0][0]:
                    new_sql['orderBy'][1][0][1][0] = new_sql['select'][1][0][0]
            else:
                for sel in new_sql['select'][1]:
                    if (sel[0] == 3 or sel[1][1][0] == 3) and new_sql['orderBy'][1][0][1][0] == 3:
                        new_sql["orderBy"][1][0] = copy.deepcopy(sel[1])
                        if not new_sql['orderBy'][1][0][1][0] and sel[0]:
                            new_sql['orderBy'][1][0][1][0] = sel[0]
            if new_sql["orderBy"][1][0][1][1] == '@':         
                for i in range(orderBy_idx-1,-1,-1):
                    sql = one_full_sql[i]
                    if sql['select'][1]:
                        new_sql["orderBy"][1][0] = sql['select'][1][0][1]
                        if not new_sql['orderBy'][1][0][1][0] and sql['select'][1][0][0]:
                            new_sql['orderBy'][1][0][1][0] = sql['select'][1][0][0]
            if new_sql["orderBy"][1][0][1][1] == '@':
                for i in range(orderBy_idx,len(one_full_sql),1):
                    sql = one_full_sql[i]
                    if sql['select'][1]:
                        new_sql["orderBy"][1][0] = sql['select'][1][0][1]
                        if not new_sql['orderBy'][1][0][1][0] and sql['select'][1][0][0]:
                            new_sql['orderBy'][1][0][1][0] = sql['select'][1][0][0]
        
        if new_sql["orderBy"]:
            no_agg_in_select = True
            for select in new_sql['select'][1]:
                if select[1][1][1] == new_sql["orderBy"][1][0][1][1] and not select[0] and not select[1][1][0]:
                    select[0] = new_sql["orderBy"][1][0][1][0]
                if select[0] or select[1][1][0]:
                    no_agg_in_select = False
            no_agg_subquery_in_where = True
            for where in new_sql['where']:
                if type(where) == list and (where[2][1][0] or type(where[3]) == list):
                    no_agg_subquery_in_where = False
                    break

            if no_agg_in_select and no_agg_subquery_in_where and  len(new_sql["orderBy"][1]) == 1 and not new_sql["orderBy"][1][0][1][0] and new_sql["limit"] == 1 and new_sql["groupBy"]:
                if new_sql["orderBy"][0] == 'asc':
                    new_sql['select'][1].append([0, [0, [2,orderBy[0][1][0][1][1],False], None]])
                else:
                    new_sql['select'][1].append([0, [0, [1,orderBy[0][1][0][1][1],False], None]])
                new_sql["orderBy"] = None
                new_sql["limit"] = None



        if check_orderBy(new_sql,schema.original_table):
            return True
        return False

    def add_where(one_full_sql,new_sql,schema,sq,used_value,fill_value):
        # fill value slot
        for (idx,sql), pts in zip(enumerate(one_full_sql),sq.pattern_tok):
            if sql['where']:
                if idx > 0 and len(sql['where']) == 1 and sql['where'][0][1] == 15 and (col_in_select_tables(sql['where'][0][3][1],one_full_sql[idx-1]['select'],schema) or (not one_full_sql[idx-1]['select'][1] and one_full_sql[idx-1]['extra'] and one_full_sql[idx-1]['extra'][1] != '@' and schema.column_names_original[one_full_sql[idx-1]['extra'][1]][0] ==  schema.column_names_original[sql['where'][0][3][1]][0])):
                    one_full_sql[idx]["where"] = []
                    continue
                elif idx + 1 < len(one_full_sql) and len(sql['where']) == 1 and sql['where'][0][1] == 15 and (col_in_select_tables(sql['where'][0][3][1],one_full_sql[idx+1]['select'],schema) or (not one_full_sql[idx+1]['select'][1] and one_full_sql[idx+1]['extra'] and schema.column_names_original[one_full_sql[idx+1]['extra'][1]][0] ==  schema.column_names_original[sql['where'][0][3][1]][0])):
                    one_full_sql[idx]["where"] = []
                    continue
                elif idx + 1 < len(one_full_sql) and len(sql['where']) == 1 and sql['where'][0][1] == 15 and (one_full_sql[idx+1]['where'] and type(one_full_sql[idx+1]['where'][0]) == list and type(one_full_sql[idx+1]['where'][0][2][1][1]) == int and schema.column_names_original[one_full_sql[idx+1]['where'][0][2][1][1]][0] ==  schema.column_names_original[sql['where'][0][3][1]][0]):
                    one_full_sql[idx]["where"] = []
                    continue
                table_json = schema.original_table
                sql_dict = sql
                for i,where in enumerate(sql_dict['where']):
                    if type(where) != list or type(where[2][1][1]) != int or where[2][1][1] < 0:
                        continue
                    elif where[3] in ["'terminal'",'value']:
                        where[3] = '"terminal"'
                if fill_value:
                    while True:
                        for i,where in enumerate(sql_dict['where']):
                            if type(where) != list or type(where[2][1][1]) != int or where[2][1][1] < 0:
                                continue
                            elif where[1] in [1,3,4,5,6,2,7,14] and (where[1] in [1,3,4,5,6] or where[2][1][0] or table_json['column_types'][where[2][1][1]] in ["number","year","time"]) and where[3] == '"terminal"':
                                success,used_value,sql_dict = analyse_num(sq,used_value,sql_dict,i,where,["NUM","DATE","YEAR"],table_json,sq_sub_idx=idx)
                                if not success:
                                    success,used_value,sql_dict = analyse_num(sq,used_value,sql_dict,i,where,["DB","SDB"],table_json,sq_sub_idx=idx)
                                if not success and len(sql_dict['where']) == 1 and where[1] == 2:
                                    success,used_value,sql_dict = analyse_num(sq,used_value,sql_dict,i,where,["NOT"],table_json,sq_sub_idx=idx)
                                if not success and i-2>=0 and type(sql_dict['where'][i-2]) == list and (sql_dict['where'][i-2][1] in [1,3,4,5,6] or sql_dict['where'][i-2][2][1][0] or (type(sql_dict['where'][i-2][2][1][1]) == int and table_json['column_types'][sql_dict['where'][i-2][2][1][1]] in ["number","year","time"])) and type(sql_dict['where'][i-2][3]) != list and sql_dict['where'][i-2][3] != '"terminal"':
                                    sql_dict['where'][i][3] = sql_dict['where'][i-2][3]
                                    success = True
                                if not success and i-4>=0 and type(sql_dict['where'][i-4]) == list and sql_dict['where'][i-4][1] == sql_dict['where'][i][1] and sql_dict['where'][i][2][1][1] == sql_dict['where'][i-4][2][1][1] and type(sql_dict['where'][i-4][3]) != list and sql_dict['where'][i-4][3] != '"terminal"':
                                    sql_dict['where'][i][3] = sql_dict['where'][i-4][3]
                                    success = True
                                if not success:
                                    if table_json['data_samples'][where[2][1][1]] and type(table_json['data_samples'][where[2][1][1]][0]) != str:
                                        sql_dict['where'][i][3] = table_json['data_samples'][where[2][1][1]][0]
                                    else:
                                        sql_dict['where'][i][3] = 1
                            elif where[1] in [2,7] and  table_json['column_types'][where[2][1][1]] in ["text"] and where[3] == '"terminal"':
                                success,used_value,sql_dict = analyse_num(sq,used_value,sql_dict,i,where,["TEXT_DB","DB"],table_json,sq_sub_idx=idx)
                                if not success and no_more_num_cond(sql_dict['where'],i,table_json):
                                    success,used_value,sql_dict = analyse_num(sq,used_value,sql_dict,i,where,["DB_NUM","NUM"],table_json,sq_sub_idx=idx)
                                if not success:
                                    success = search_db(sq,table_json,where,used_value,sq_sub_idx=idx)
                                if not success and i-4>=0 and type(sql_dict['where'][i-4]) == list and sql_dict['where'][i-4][1] == sql_dict['where'][i][1] and sql_dict['where'][i][2][1][1] == sql_dict['where'][i-4][2][1][1] and type(sql_dict['where'][i-4][3]) != list and sql_dict['where'][i-4][3] != '"terminal"':
                                    sql_dict['where'][i][3] = sql_dict['where'][i-4][3]
                                    success = True

                        # BOOL
                        for i,where in enumerate(sql_dict['where']):
                            if type(where) != list or type(where[2][1][1]) != int or where[2][1][1] < 0:
                                continue
                            elif where[1] in [2,7] and  table_json['column_types'][where[2][1][1]] in ["boolean"] and where[3] == '"terminal"':
                                success,used_value,sql_dict = analyse_num(sq,used_value,sql_dict,i,where,["DB"],table_json,sq_sub_idx=idx)
                                if not success:
                                    success,used_value,sql_dict = analyse_num(sq,used_value,sql_dict,i,where,["PDB"],table_json,sq_sub_idx=idx)
                                if not success:
                                    success,used_value,sql_dict = analyse_num(sq,used_value,sql_dict,i,where,["UDB"],table_json,sq_sub_idx=idx)
                                if not success:
                                    success,used_value,sql_dict = analyse_num(sq,used_value,sql_dict,i,where,["SDB"],table_json,sq_sub_idx=idx)
                                if not success:
                                    success,used_value,sql_dict = analyse_num(sq,used_value,sql_dict,i,where,["BOOL_NUM","NUM"],table_json,sq_sub_idx=idx)
                                if not success:
                                    search_bcol(sq,table_json,where,sql_dict,i)
                                if type(where[3]) == str and where[3][1:-1] not in table_json["data_samples"][where[2][1][1]]:
                                    for v in table_json["data_samples"][where[2][1][1]]:
                                        if type(v) == str and (where[3][1:].lower().startswith(v.lower()) or v.lower().startswith(where[3][1:].lower())):
                                            where[3] = "'"+v+"'"
                                if i == 2 and len(sql_dict['where']) == 3 and type(sql_dict['where'][0]) == list and sql_dict['where'][0][2][1][1] == where[2][1][1] and where[3] == sql_dict['where'][0][3]:
                                    for v in table_json["data_samples"][where[2][1][1]]:
                                        if v != where[3][1:-1]:
                                            if type(v) == str:
                                                where[3] = "'"+v+"'"
                                            else:
                                                where[3] = v
                                            break
                                if type(where[3]) == str and where[3][1:-1] not in table_json["data_samples"][where[2][1][1]] and table_json["data_samples"][where[2][1][1]]:
                                    if where[3].startswith("'") and where[3].endswith("'") and not str_is_num(where[3][1:-1]) and table_json["data_samples"][where[2][1][1]]:
                                        success,used_value,sql_dict = analyse_num(sq,used_value,sql_dict,i,where,["BOOL_NUM","NUM"],table_json,sq_sub_idx=idx)
                                        if not success:
                                            where[3] = '"terminal"'
                                            search_bcol(sq,table_json,where,sql_dict,i)

                        for i,where in enumerate(sql_dict['where']):
                            if type(where) != list or type(where[2][1][1]) != int or where[2][1][1] < 0:
                                continue
                            elif where[1] in [9,13] and where[3] == '"terminal"':
                                success,used_value,sql_dict = analyse_num(sq,used_value,sql_dict,i,where,["PDB"],table_json,sq_sub_idx=idx)
                                if not success:
                                    success,used_value,sql_dict = analyse_num(sq,used_value,sql_dict,i,where,["UDB"],table_json,sq_sub_idx=idx)
                                if not success:
                                    success,used_value,sql_dict = analyse_num(sq,used_value,sql_dict,i,where,["tilt"],table_json,sq_sub_idx=idx)
                                if not success:
                                    success,used_value,sql_dict = analyse_num(sq,used_value,sql_dict,i,where,["SDB"],table_json,sq_sub_idx=idx)
                                if not success:
                                    success,used_value,sql_dict = analyse_num(sq,used_value,sql_dict,i,where,["DB"],table_json,sq_sub_idx=idx)
                                if not success:
                                    success,used_value,sql_dict = analyse_num(sq,used_value,sql_dict,i,where,["NUM","DATE","YEAR"],table_json,sq_sub_idx=idx)
                                    if success:
                                        where[3] = "'" + str(where[3]) + "'"
                                if not success:
                                    where[3] = "'%'"
                                else:
                                    if table_json['column_types_checked'][where[2][1][1]] in ["date","year","time","number"]:
                                        if type(where[3]) == str and where[3].startswith("'"):
                                            stemmer = MyStemmer()
                                            value=stemmer.restem(where[3][1:-1])
                                            if value.isdigit():
                                                where[3] = "'"+value+"%'"
                                            elif str2num(where[3][1:-1]):
                                                where[3] = "'"+str(str2num(where[3][1:-1]))+"%'"
                                            elif "%" not in where[3]:
                                                where[3] = where[3][0] + "%" + where[3][1:-1] + "%" + where[3][-1]
                                        else:
                                            where[3] =  "'" + str(where[3]) + "%'"

                                if "%" not in where[3] and len(where[3]) > 2:
                                    if where[3][1:-1].isdigit() or " start with " in sq.sub_sequence_list[idx] or " starting with " in sq.sub_sequence_list[idx] or " started with " in sq.sub_sequence_list[idx] or " starts with " in sq.sub_sequence_list[idx] or " begin with " in sq.sub_sequence_list[idx] or " beginning with " in sq.sub_sequence_list[idx] or " began with " in sq.sub_sequence_list[idx] or " begins with " in sq.sub_sequence_list[idx] or " start from " in sq.sub_sequence_list[idx] or " starts from " in sq.sub_sequence_list[idx] or " starting from " in sq.sub_sequence_list[idx] or " started from " in sq.sub_sequence_list[idx] or " begin from " in sq.sub_sequence_list[idx] or " begins from " in sq.sub_sequence_list[idx] or " began from " in sq.sub_sequence_list[idx] or " beginning from " in sq.sub_sequence_list[idx]:
                                        where[3] = where[3][0:-1] + "%" + where[3][-1]
                                    elif " end with " in sq.sub_sequence_list[idx] or " ending with " in sq.sub_sequence_list[idx] or " ended with " in sq.sub_sequence_list[idx] or " ends with " in sq.sub_sequence_list[idx] or " end by " in sq.sub_sequence_list[idx] or " ending by " in sq.sub_sequence_list[idx] or " ended by " in sq.sub_sequence_list[idx] or " ends by " in sq.sub_sequence_list[idx]:
                                        where[3] =  where[3][0] + "%" + where[3][1:]
                                    elif " contain " in sq.sub_sequence_list[idx] or " containing " in sq.sub_sequence_list[idx] or " contained " in sq.sub_sequence_list[idx] or " including " in sq.sub_sequence_list[idx] or " include " in sq.sub_sequence_list[idx] or " included " in sq.sub_sequence_list[idx]:
                                        where[3] = where[3][0] + "%" + where[3][1:-1] + "%" + where[3][-1]
                                    elif where[3][1:-1].isdigit() or " start with " in sq.question_or or " starting with " in sq.question_or or " started with " in sq.question_or or " starts with " in sq.question_or or " begin with " in sq.question_or or " beginning with " in sq.question_or or " began with " in sq.question_or or " begins with " in sq.question_or or " start from " in sq.question_or or " starts from " in sq.question_or or " starting from " in sq.question_or or " started from " in sq.question_or or " begin from " in sq.question_or or " begins from " in sq.question_or or " began from " in sq.question_or or " beginning from " in sq.question_or:
                                        where[3] = where[3][0:-1] + "%" + where[3][-1]
                                    elif " end with " in sq.question_or or " ending with " in sq.question_or or " ended with " in sq.question_or or " ends with " in sq.question_or or " end by " in sq.question_or or " ending by " in sq.question_or or " ended by " in sq.question_or or " ends by " in sq.question_or:
                                        where[3] =  where[3][0] + "%" + where[3][1:]
                                    else:
                                        where[3] = where[3][0] + "%" + where[3][1:-1] + "%" + where[3][-1]

                        if 'DB' in pts:
                            dbm_idx_set = []
                            for fdbm in sq.full_db_match[idx]:
                                dbm_idx_set.extend(fdbm)
                            dbm_idx_set = list(set(dbm_idx_set)) 
                            if len(dbm_idx_set) == 1:   
                                if len(sql_dict['where']) == 1 and sql_dict['where'][0][3] == '"terminal"' and schema.column_tokens_table_idx[sql_dict['where'][0][2][1][1]] == schema.column_tokens_table_idx[reversed_link_back_col(dbm_idx_set[0],schema.original_table)] and sql_dict['where'][0][2][1][1] != reversed_link_back_col(dbm_idx_set[0],schema.original_table):
                                    sql_dict['where'][0][2][1][1] = reversed_link_back_col(dbm_idx_set[0],schema.original_table)
                                    continue
                                elif len(sql_dict['where']) == 3 and pts.count("DB") >= 2 and (sql_dict['where'][0][3] == '"terminal"' or sql_dict['where'][2][3] == '"terminal"') and (sql_dict['where'][0][3] != '"terminal"' or sql_dict['where'][2][3] != '"terminal"') and (sql_dict['where'][0][2][1][1] == reversed_link_back_col(dbm_idx_set[0],schema.original_table) or sql_dict['where'][2][2][1][1] == reversed_link_back_col(dbm_idx_set[0],schema.original_table)) and (sql_dict['where'][0][2][1][1] != reversed_link_back_col(dbm_idx_set[0],schema.original_table) or sql_dict['where'][2][2][1][1] != reversed_link_back_col(dbm_idx_set[0],schema.original_table)):
                                    sql_dict['where'][0][2][1][1] = reversed_link_back_col(dbm_idx_set[0],schema.original_table)
                                    sql_dict['where'][2][2][1][1] = reversed_link_back_col(dbm_idx_set[0],schema.original_table)
                                    continue
                        break
                #################################### Fill Value End
                
                if idx > 0 and one_full_sql[idx-1]['extra'] and not sql['extra'] and not one_full_sql[idx-1]['where'] and not one_full_sql[idx-1]['orderBy'] and one_full_sql[idx-1]['extra'][0] == 0 and (one_full_sql[idx-1]['extra'][1] in schema.foreignKey or one_full_sql[idx-1]['extra'][1] in schema.primaryKey) :
                    sql['extra'] = one_full_sql[idx-1]['extra']
                    one_full_sql[idx-1]['extra'] = None
                elif idx > 0 and one_full_sql[idx-1]['extra'] and sql['where'] and type(sql['where'][0]) == list and (sql['where'][0][2][1][1] == one_full_sql[idx-1]['extra'][1] or type(sql['where'][0][2][1][1]) != int):
                    sql['where'][0][2][1][1] == one_full_sql[idx-1]['extra'][1]
                    one_full_sql[idx-1]['extra'] = None
                if sql['extra'] and sql['extra'][0] == 0 and (sql['extra'][1] in schema.foreignKey or sql['extra'][1] in schema.primaryKey):
                    new_where = [False, 8, [0, [0,'@.@',False], None], [0,0,False], None]
                    pts_str = " ".join([p for p in pts if p not in ["#","*"]])
                    if pts_str.endswith("and COL") or pts_str.endswith("and COL COL"):
                        new_sql['select'][1].append([0, [0, sql['extra'], None]])
                        one_full_sql[idx]['extra'] = None
                    elif col_in_select_tables(sql['extra'][1],new_sql['select'],schema):
                        new_where[2][1] = sql['extra']
                        insert_idx,col_right = generate_right_col_for_where(sql['extra'][1],sql['where'],schema)
                        if insert_idx >= 0:
                            new_where[3][1] = col_right
                            sql['where'].insert(insert_idx,"and")
                            sql['where'].insert(insert_idx,new_where)
                            one_full_sql[idx]['extra'] = None
                    else:
                        new_where[3] = sql['extra']
                        sql['where'].insert(0,"and")
                        sql['where'].insert(0,new_where)
                        one_full_sql[idx]['extra'] = None

            if len(sql['where']) == 3 and sql['where'][1] == "and" and sql['where'][0][1] == 2 and type(sql['where'][0][3]) != list and type(sql['where'][2][3]) == list and sql['where'][2][3][0] and sql['where'][2][1] != 2:
                sql['where'].append("and")
                sql['where'].append("-4")
        
        # combine where condition:
        for idx,sql in enumerate(one_full_sql):
            if sql['where']:
                if len(sql['where']) == 3 and sql['where'][0][2][1][1] == sql['where'][2][2][1][1]  and sql['where'][2][3] == '"terminal"' and sq.sub_sequence_toks[idx][-1] in ["and","or"]:
                    sql['where'].pop(-1)
                    sql['where'].pop(-1)
                for w in sql['where']:
                    if type(w) == list and (w not in new_sql['where'] or w[1] in [8,12] or (len(new_sql['where']) > 4 and type(w[2][1][1]) == int and schema.column_types[w[2][1][1]] == 'boolean' and new_sql['where'][-2] != w )):
                        if new_sql['where'] and type(new_sql['where'][-1]) == list:
                            if "or" == sq.pattern_tok[idx][0] or (len(sq.pattern_tok[idx]) > 2 and "or" == sq.pattern_tok[idx][1] and "#" == sq.pattern_tok[idx][0])  or (idx>0 and "or" == sq.pattern_tok[idx-1][-1]) or (len(sq.pattern_tok[idx-1]) > 2 and "or" == sq.pattern_tok[idx-1][-2] and "#" == sq.pattern_tok[idx-1][-1]):
                                new_sql['where'].append("or")
                            elif idx >= 2 and not one_full_sql[idx-1]['where'] and one_full_sql[idx-2]['where'] and ("or" == sq.pattern_tok[idx-2][-1] or (len(sq.pattern_tok[idx-1]) > 2 and "or" == sq.pattern_tok[idx-2][-2] and "#" == sq.pattern_tok[idx-2][-1])):
                                new_sql['where'].append("or")
                            else:
                                new_sql['where'].append("and")
                        new_sql['where'].append(w)
                    elif type(w) == str:
                        if w == "-4":
                            new_sql['where'].append(new_sql['where'][-4])
                        else:
                            new_sql['where'].append(w)
        if new_sql['where'] and type(new_sql['where'][-1]) == str:
            new_sql['where'].pop(-1)
        if len(new_sql['where']) == 3 and new_sql['where'][0][2][1][1] == new_sql['where'][2][2][1][1]  and new_sql['where'][0][3] == '"terminal"' and new_sql['where'][0][3] != new_sql['where'][2][3]:
            new_sql['where'].pop(0)
            new_sql['where'].pop(0)
        # analyse @ column:
        for i,w in enumerate(new_sql['where']):
            if type(w) == list and i > 1 and type(new_sql['where'][i-2]) == list and w[2][1][1] in ["@","@.@"] and type(w[3]) != list :
                w[2][1][1] = new_sql['where'][i-2][2][1][1]
                w[2][1][0] = new_sql['where'][i-2][2][1][0]
        if len(new_sql['where']) == 5 and new_sql['where'][1] == 'or' and new_sql['where'][4][1] == 15:
            new_sql['where'].insert(0,new_sql['where'][4])
            new_sql['where'].insert(0,'and')
            new_sql['where'].pop(-1)
            new_sql['where'].pop(-1)
        if len(new_sql['where']) == 2 and new_sql['where'][0] in ['except_',"except"] and new_sql['where'][1][1] != 10:
            if type(new_sql['where'][1][2][1][1]) == int:
                col_id = schema.tbl_col_idx_back[schema.column_tokens_table_idx[new_sql['where'][1][2][1][1]]][0]
            else:
                col_id = schema.tbl_col_idx_back[schema.column_tokens_table_idx[new_sql['where'][1][3][1]]][0]
            new_sql['where'].insert(1,'and')
            new_where = [False, 10, [0, [0,'@.@',False], None], [0,col_id,False], None]
            new_sql['where'].insert(1,new_where)
        if check_where(new_sql,schema):
            return True
        return False


    
    def add_IUE(one_full_sql,new_sql,schema):
        for idx,sql in enumerate(one_full_sql):
            if sql['union']:
                new_sql['union'] =  {'limit':None,'intersect':None,'union':None,'except':None,'having':[],'orderBy':[],'groupBy':[],'where':[],"select":[False,[]]}
                if type(sql['union']) == dict:
                    new_sql['union']["select"] = sql['union']["select"]
                else:
                    new_sql['union']["select"] = [False,[sql['union']]]
                new_sql['union']["from"] = {"conds": [],"table_units": [["table_unit",schema.column_tokens_table_idx[new_sql['union']['select'][1][0][1][1][1]]]]}

            if sql['intersect']:
                new_sql['intersect'] =  {'limit':None,'intersect':None,'union':None,'except':None,'having':[],'orderBy':[],'groupBy':[],'where':[],"select":[False,[]]}
                if type(sql['intersect']) == dict:
                    new_sql['intersect']["select"] = sql['intersect']["select"]
                else:
                    new_sql['intersect']["select"] = [False,[sql['intersect']]]
                new_sql['intersect']["from"] = {"conds": [],"table_units": [["table_unit",schema.column_tokens_table_idx[new_sql['intersect']['select'][1][0][1][1][1]]]]}
    
    def analyse_extra_col(one_full_sql,new_sql,schema,sq):
        for idx,sql in enumerate(one_full_sql):
            if sql['extra']:
                next_is_empty = True if idx + 1 == len(one_full_sql) or (not one_full_sql[idx+1]['select'][1] and not one_full_sql[idx+1]['orderBy'] and not one_full_sql[idx+1]['where'] and not one_full_sql[idx+1]['groupBy'] and not one_full_sql[idx+1]['extra'] and not one_full_sql[idx+1]['limit'] and not one_full_sql[idx+1]['union'] and not one_full_sql[idx+1]['intersect']) else False
                if idx > 0 and type(sql['extra'][1]) == int and (not one_full_sql[idx-1]['orderBy'] or one_full_sql[idx-1]['limit']) and (( not one_full_sql[idx-1]['select'][1] and next_is_empty and not sql['where'] and not sql['orderBy']) or (one_full_sql[idx-1]['select'][1] and not one_full_sql[idx-1]['where'] and not one_full_sql[idx-1]['orderBy'] and not one_full_sql[idx]['where'] and not one_full_sql[idx]['orderBy']) or (one_full_sql[idx-1]['orderBy'] and one_full_sql[idx-1]['limit'] and not one_full_sql[idx-1]['select'][1] and next_is_empty and not one_full_sql[idx]['where'] and not one_full_sql[idx]['orderBy'])):
                    col_already_contain = False
                    col_agg_already_contain = False
                    for col in new_sql['select'][1]:
                        agg = col[0] if col[0] else col[1][1][0]
                        if agg == sql['extra'][0] and sql['extra'][1] == col[1][1][1]:
                            col_agg_already_contain = True
                        if sql['extra'][1] == col[1][1][1]:
                            col_already_contain = True

                    if not col_agg_already_contain:
                        continue_words = False
                        if not (sq.sub_sequence_list[idx-1].endswith("and") or sq.sub_sequence_list[idx-1].endswith(" ,") or sq.sub_sequence_list[idx-1].endswith("or") or sq.sub_sequence_list[idx-1].endswith("and ,") or sq.sub_sequence_list[idx-1].endswith(" along with") or sq.sub_sequence_list[idx-1].endswith(" as well as") or sq.sub_sequence_list[idx].startswith("and ") or sq.sub_sequence_list[idx].startswith(", ") or sq.sub_sequence_list[idx].startswith("or ") or sq.sub_sequence_list[idx].startswith("as well as ") or sq.sub_sequence_list[idx].startswith(", as well as ") or sq.sub_sequence_list[idx].startswith(", and ") or sq.sub_sequence_list[idx].startswith("along with ") or sq.sub_sequence_list[idx].startswith("together with ") or sq.sub_sequence_list[idx].startswith("with ") or sq.sub_sequence_list[idx].startswith(", along with ")):
                            if not col_agg_already_contain and one_full_sql[idx-1]['where'] and len(one_full_sql[idx-1]['where']) == 1 and (new_sql['where'][0][2][1][1] == sql['extra'][1] or (type(new_sql['where'][0][3]) == list and new_sql['where'][0][3][1] == sql['extra'][1])) and not continue_words:
                                continue 
                            if (sql['extra'][1] in schema.foreignKey or sql['extra'][1] in schema.primaryKey) and not continue_words:
                                continue
                        if len(new_sql['select'][1]) == 1 and (sq.sub_sequence_toks[idx-1][-1] == 'or' or sq.sub_sequence_toks[idx][0] == 'or'): 
                            new_sql['union'] =  {'limit':None,'intersect':None,'union':None,'except':None,'having':[],'orderBy':[],'groupBy':[],'where':[],"select":[False,[]]}
                            new_sql['union']['select'][1].append([0, [0, sql['extra'], None]])
                            new_sql['union']["from"] = {"conds": [],"table_units": [["table_unit",schema.column_tokens_table_idx[new_sql['union']['select'][1][0][1][1][1]]]]}
                        else:
                            new_sql['select'][1].append([0, [0, sql['extra'], None]])
                        continue
                elif idx > 0 and sql['extra'] == one_full_sql[idx-1]['extra']:
                    continue
                elif type(sql['extra'][1]) != int and len(new_sql['select'][1]) == 1 and not new_sql['select'][1][0][0] and not new_sql['select'][1][0][1][1][0]:
                    new_sql['select'][1][0][0] = sql['extra'][0]
                    continue
                elif idx > 0 and ((one_full_sql[idx-1]['orderBy'] and not one_full_sql[idx-1]['limit'] and sql['extra'] != one_full_sql[idx-1]['orderBy'][1][0][1] and not one_full_sql[idx-1]['select'][1] and next_is_empty and not one_full_sql[idx]['where'] and not one_full_sql[idx]['orderBy'])):
                    new_sql['orderBy'][1].append([0, sql['extra'], None])
                    continue
                elif idx > 0 and (sq.sub_sequence_list[idx-1].endswith("and") or sq.sub_sequence_list[idx-1].endswith("and ,") or sq.sub_sequence_list[idx-1].endswith(" along with") or sq.sub_sequence_list[idx-1].endswith(" as well as") or sq.sub_sequence_list[idx].startswith("and ") or sq.sub_sequence_list[idx].startswith("as well as ") or sq.sub_sequence_list[idx].startswith(", as well as ") or sq.sub_sequence_list[idx].startswith(", and ") or sq.sub_sequence_list[idx].startswith("along with ") or sq.sub_sequence_list[idx].startswith("together with ") or sq.sub_sequence_list[idx].startswith("with ") or sq.sub_sequence_list[idx].startswith(", along with ")):
                    new_sql['select'][1].append([0, [0, sql['extra'], None]])

    def analyse_bcol_col(one_full_sql,new_sql,schema,sq,used_value):
        for idx,sql in enumerate(one_full_sql):
            if 'bcol' in sql and sql['bcol'] and schema.column_types[sql['bcol'][1]] == 'boolean':
                col_already_contain = False
                for where in new_sql['where']:
                    if type(where) == list and where[2][1][1] == sql['bcol'][1]:
                        col_already_contain = True
                if not col_already_contain:
                    cond_idx = 0
                    one_more_condition = False
                    if len(new_sql['where']) == 3 and type(new_sql['where'][0]) == list and type(new_sql['where'][2]) == list and new_sql['where'][0][2][1][1] == new_sql['where'][2][2][1][1] and type(new_sql['where'][0][2][1][1]) == int:
                        no_agg_in_select = True
                        for select in new_sql['select'][1]:
                            if select[0] or select[1][1][0]:
                                no_agg_in_select = False
                        one_more_condition = no_agg_in_select
                        new_sql['where'].insert(0,"and")
                        new_sql['where'].insert(0,[False, 2, [0, [0, sql['bcol'][1], False], None], '"terminal"', None])
                    elif new_sql['where'] and (len(new_sql['where']) == 1 and type(new_sql['where'][0]) == list and new_sql['where'][0][1] in [12,8]) or (len(new_sql['where']) == 3 and type(new_sql['where'][0]) == list and new_sql['where'][0][1] in [12,8] and new_sql['where'][2][1] == 15):
                        new_sql['where'].insert(0,"and")
                        new_sql['where'].insert(0,[False, 2, [0, [0, sql['bcol'][1], False], None], '"terminal"', None])
                    elif new_sql['where']:
                        new_sql['where'].append("and")
                        new_sql['where'].append([False, 2, [0, [0, sql['bcol'][1], False], None], '"terminal"', None])
                        cond_idx = len(new_sql['where']) - 1
                    else:
                        new_sql['where'].append([False, 2, [0, [0, sql['bcol'][1], False], None], '"terminal"', None])
                    where = new_sql['where'][cond_idx]
                    table_json = schema.original_table
                    sql_dict  = new_sql
                    success,used_value,sql_dict = analyse_num(sq,used_value,sql_dict,cond_idx,where,["DB"],table_json,sq_sub_idx=idx)
                    if not success:
                        success,used_value,sql_dict = analyse_num(sq,used_value,sql_dict,cond_idx,where,["PDB"],table_json,sq_sub_idx=idx)
                    if not success:
                        success,used_value,sql_dict = analyse_num(sq,used_value,sql_dict,cond_idx,where,["UDB"],table_json,sq_sub_idx=idx)
                    if not success:
                        success,used_value,sql_dict = analyse_num(sq,used_value,sql_dict,cond_idx,where,["SDB"],table_json,sq_sub_idx=idx)
                    if not success:
                        success,used_value,sql_dict = analyse_num(sq,used_value,sql_dict,cond_idx,where,["BOOL_NUM","NUM"],table_json,sq_sub_idx=idx)
                    if not success:
                        search_bcol(sq,table_json,where,sql_dict,cond_idx)
                    if type(where[3]) == str and where[3][1:-1] not in table_json["data_samples"][where[2][1][1]]:
                        for v in table_json["data_samples"][where[2][1][1]]:
                            if type(v) == str and (where[3][1:].lower().startswith(v.lower()) or v.lower().startswith(where[3][1:].lower())):
                                where[3] = "'"+v+"'"
                    if cond_idx == 2 and len(sql_dict['where']) == 3 and type(sql_dict['where'][0]) == list and sql_dict['where'][0][2][1][1] == where[2][1][1] and where[3] == sql_dict['where'][0][3]:
                        for v in table_json["data_samples"][where[2][1][1]]:
                            if v != where[3][1:-1]:
                                if type(v) == str:
                                    where[3] = "'"+v+"'"
                                else:
                                    where[3] = v
                                break
                    if type(where[3]) == str and where[3][1:-1] not in table_json["data_samples"][where[2][1][1]] and table_json["data_samples"][where[2][1][1]]:
                        if where[3].startswith("'") and where[3].endswith("'") and not str_is_num(where[3][1:-1]) and table_json["data_samples"][where[2][1][1]]:
                            success,used_value,sql_dict = analyse_num(sq,used_value,sql_dict,cond_idx,where,["BOOL_NUM","NUM"],table_json,sq_sub_idx=idx)
                            if not success:
                                where[3] = '"terminal"'
                                search_bcol(sq,table_json,where,sql_dict,cond_idx)
                    if one_more_condition:
                        new_sql['where'].insert(4,"and")
                        new_sql['where'].insert(4,where)


    def generate_sql_clause(add_fun,one_full_sql,beam_idsx,sub_sqls,new_sql,schema,clause_name,empty_value,sq,used_value,fill_value):
        tmp_store = None
        used_value_tmp = copy.deepcopy(used_value)
        if not add_fun(sub_sqls,new_sql,schema,sq,used_value,fill_value):
            tmp_store = copy.deepcopy(new_sql[clause_name])
            for (last_score, tmp_beam_idsx, sub_sqls) in next_beam(beam_idsx,clause_name,one_full_sql):
                used_value = copy.deepcopy(used_value_tmp)
                new_sql[clause_name] = copy.deepcopy(empty_value)
                if add_fun(sub_sqls,new_sql,schema,sq,used_value,fill_value):
                    tmp_store = None
                    beam_idsx = tmp_beam_idsx
                    break
        if tmp_store:
            new_sql[clause_name] = tmp_store
            sub_sqls = [b[0] for b in one_full_sql]
        return sub_sqls,beam_idsx

    ############################################
    ############################################
    ############################################

    assert len(one_full_sql) == len(sq.sub_sequence_type)
    new_sql = {
        'limit':None,
        'intersect':None,
        'union':None,
        'except':None,
        'having':[],
        'orderBy':[],
        'groupBy':[],
        'where':[],
        "select":[False,[]],
    }
    #combine select
    
    last_score = sum([b[beam_idsx[i]]['score'] for i,b in enumerate(one_full_sql)])
    sub_sqls = [b[beam_idsx[i]] for i,b in enumerate(one_full_sql)]

    sub_sqls,beam_idsx = generate_sql_clause(add_select,one_full_sql,beam_idsx,sub_sqls,new_sql,schema,"select",[False,[]],sq,None,fill_value)

    #combine group by
    add_groupBy(sub_sqls,new_sql,schema)

    used_value = []
    
    #combine where
    sub_sqls,beam_idsx = generate_sql_clause(add_where,one_full_sql,beam_idsx,sub_sqls,new_sql,schema,"where",[],sq,used_value,fill_value)
    analyse_bcol_col(sub_sqls,new_sql,schema,sq,used_value)

    #combine order by 
    sub_sqls,beam_idsx = generate_sql_clause(add_orderBy,one_full_sql,beam_idsx,sub_sqls,new_sql,schema,"orderBy",[],sq,used_value,fill_value)


    #combine IUE
    add_IUE(sub_sqls,new_sql,schema)

    remove_groupBY(sub_sqls,new_sql,schema)
    new_sql["from"] = dict({"conds": [],"table_units": [["table_unit",0]]})
    analyse_extra_col(sub_sqls,new_sql,schema,sq)

    if len(new_sql['where']) == 3 and new_sql['where'][1] == "except_" and new_sql['where'][2][1] == 10:
        new_sql['where'][2][1] = 15
    natsql = sql_back(new_sql,schema.original_table)
    db_name = schema.db_id
    try:
        args=Args()
        args.not_infer_group = False
        final_sql,p_nsql,__ = create_sql_from_natSQL(natsql.replace(" union_ "," union ").replace(" intersect_ "," intersect "), db_name, database+db_name+"/"+db_name+".sqlite", schema.original_table, sq=sq, remove_groupby_from_natsql=False,args=args)
    except:
        final_sql,p_nsql = (None,None)
    return final_sql,beam_idsx,natsql,p_nsql