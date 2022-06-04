import json,argparse,copy,os
from natsql_sp2sql import generate_natsql_from_split_data
from natsql2sql.preprocess.sq import SubQuestion
from natsql2sql.preprocess.Schema_Token import Schema_Token
from natsql2sql.preprocess.TokenString import get_spacy_tokenizer
from natsql2sql.preprocess.sql_back import WHERE_OPS,AGG_OPS
from natsql2sql.natsql_parser import Schema_Num
from natsql2sql.process_sql import get_schema
from natsql2sql.process_sql import get_sql as get_original_sql

class component:
    def __init__(self, sql_or, sqls, table, sq_idxs):
        self.sql = sqls[0]
        self.sql_sps = sqls
        self.sql_or = sql_or
        self.table = table
        self.condition = [None] * 5 
        self.sq_idxs = sq_idxs

        self.sub_question = ""
        for sql in sqls:
            self.sub_question += " " + " ".join([sql['question_toks'][i] for i in range(sql['question_range'][0],sql['question_range'][1]+1)])
        self.sub_question = self.sub_question[1:]

        if len(sqls) == 1 and sqls[0]['sp_id'] == 0 and sqls[0]['sp_num'] != 1 and sqls[0]['question_toks'][sqls[0]['question_range'][1]] not in [".","?",","]:
            self.condition[4] = True
            return

        # last word match:
        if len(sqls) > 1 or (sqls[0]['sql']['where'] and sqls[0]['sql']['orderBy']) or sqls[0]['sql']['groupBy']:
            self.condition[0] = 1
        elif sql_or['question_lemma_toks'][sqls[0]['question_range'][0]] in {"what","which","who","whom","whose","that","where","when","why"}:
            self.condition[0] = 1
        # elif sqls[0]['sql']['where'] and type(sqls[0]['sql']['where'][0]) == list and type(sqls[0]['sql']['where'][0][2][1][1]) == int and table['column_names'][sqls[0]['sql']['where'][0][2][1][1]][0] not in select_tables:
        #     self.condition[0] = 1
        else:
            pass
        
        for sql in sqls:
            if sql['sql']['where']:
                for where in sql['sql']['where']:
                    if type(where) == list:
                        if (type(where[2][1][1]) == int and table['column_names'][where[2][1][1]][0] not in sql_or['select_table_idxs']) or where[1] in [8,12]:
                            self.condition[0] = 1
                    elif where in ["except","except_"]:
                        self.condition[0] = 1

        # group by match:
        for sql in sqls:
            if sql['sql']['groupBy']:
                self.condition[1] = sql['sql']['groupBy']
        if not self.condition[1] and sql_or['sql']['groupBy']:
            for sql in sqls:
                if sql['sql']['where']:
                    for where in sql['sql']['where']:
                        if type(where) == list and type(where[2][1][1]) == int and where[2][1][0]:
                            self.condition[1] = sql_or['sql']['groupBy']
        if not self.condition[1] and sql_or['sql']['groupBy'] and sqls[0]['sql']['orderBy'] and sqls[0]['sql']['orderBy'][1][0][1][0]:
            self.condition[1] = sql_or['sql']['groupBy']

        # There is orderBy
        if sqls[0]['sql']['orderBy']:
            self.condition[3] = True
        
        if len(sqls) > 1:
            self.sql = copy.deepcopy(sqls[0])
            self.sql['sp_num'] -= (len(sqls) - 1)
            for i,sql in enumerate(sqls):
                if i > 0:
                    if self.sql['question_range'][1] +1 != sql['question_range'][0]:
                        self.condition[4] = True
                    else:
                        self.sql['question_range'][1] = sql['question_range'][1]
                        self.sql['sub_question'] = " ".join([ self.sql['question_toks'][c] for c in range(self.sql['question_range'][0],self.sql['question_range'][-1]+1)])
                    if sql['sql']['where']:
                        if self.sql['sql']['where']:
                            if  sql['question_toks'][sql['question_range'][0]] == "or" or (sql['question_range'][0] + 1 <= sql['question_range'][1] and sql['question_toks'][sql['question_range'][0]+1] == "or") or (sql['question_range'][0] + 2 <= sql['question_range'][1] and sql['question_toks'][sql['question_range'][0]+2] == "or"):
                                self.sql['sql']['where'].append("or")
                            elif  sql['question_toks'][self.sql['question_range'][1]] == "or" or (self.sql['question_range'][1] - 1 >= self.sql['question_range'][0] and sql['question_toks'][self.sql['question_range'][1]-1] == "or"):
                                self.sql['sql']['where'].append("or")
                            else:
                                self.sql['sql']['where'].append("and")
                            self.sql['sql']['where'].extend(sql['sql']['where'])
                        else:
                            self.sql['sql']['where'] = sql['sql']['where']
                    if sql['sql']['orderBy']:
                        if self.sql['sql']['orderBy']:
                            if self.sql['sql']['orderBy'][1][0][1][1] != sql['sql']['orderBy'][1][0][1][1]:
                                self.sql['sql']['orderBy'][1].extend(sql['sql']['orderBy'][1])
                                assert False
                            if sql['sql']['orderBy'][0] == 'desc':
                                self.sql['sql']['orderBy'][0] = sql['sql']['orderBy'][0]
                        else:
                            self.sql['sql']['orderBy'] = sql['sql']['orderBy']
                    if sql['sql']['limit']:
                        if self.sql['sql']['limit']:
                            self.sql['sql']['limit'] = sql['sql']['limit'] if sql['sql']['limit'] > self.sql['sql']['limit'] else self.sql['sql']['limit']
                        else:
                            self.sql['sql']['limit'] = sql['sql']['limit']

    def check_replacement(self,to_be_replaced_component,schema):
        if self.sub_question[1:-1].lower() in to_be_replaced_component.sql_or['question_or'].lower():
            return False # question is similar
        
        if len(self.sql['sql']['where']) == 3 and self.sql['sql']['where'][0][2][1][1] == self.sql['sql']['where'][2][2][1][1] and type(self.sql['sql']['where'][0][2][1][1]) == int and schema.column_types[self.sql['sql']['where'][0][2][1][1]] == 'boolean':
            return False
        elif len(to_be_replaced_component.sql['sql']['where']) == 3 and to_be_replaced_component.sql['sql']['where'][0][2][1][1] == to_be_replaced_component.sql['sql']['where'][2][2][1][1] and type(to_be_replaced_component.sql['sql']['where'][0][2][1][1]) == int and schema.column_types[to_be_replaced_component.sql['sql']['where'][0][2][1][1]] == 'boolean':
            return False

        if to_be_replaced_component.sql['sql']['orderBy']:
            for i,sp in enumerate(to_be_replaced_component.sql_or['sp_data']):
                if i not in to_be_replaced_component.sq_idxs and sp['sql']['limit'] and not sp['sql']['orderBy']:
                    if not self.sql['sql']['limit'] or not self.sql['sql']['orderBy']:
                        return False
                elif i not in to_be_replaced_component.sq_idxs and not sp['sql']['limit'] and sp['sql']['orderBy'] and self.sql['sql']['where']:
                    return False # not allow order by + where


        if to_be_replaced_component.sql_or['sql']['where'] and self.sql['sql']['where']:

            select_tables = []
            agg_in_select = False
            for select in to_be_replaced_component.sql_or['sql']['select'][1]:
                select_tables.append(schema.column_tokens_table_idx[select[1][1][1]])
                if select[0] or select[1][1][0]:
                    agg_in_select = True
            select_tables = list(set(select_tables))

            for w_i,where in enumerate(self.sql['sql']['where']):
                if type(where) == list and where[1] != 15 and where in to_be_replaced_component.sql_or['sql']['where']:
                    return False # condition redundant
                if type(where) == list:
                    for w in to_be_replaced_component.sql_or['sql']['where']:
                        if type(w) == list:
                            if w not in to_be_replaced_component.sql['sql']['where']:
                                if agg_in_select and ((len(self.sql['sql']['where']) == 3 and self.sql['sql']['where'][0][2][1][1] == self.sql['sql']['where'][2][2][1][1]) or len(self.sql['sql']['where']) > 3):
                                    return False # Prevention of complex IUE
                                elif w[2][1][0] and where[2][1][0] and (where[2][1][0] != where[2][1][0] or where[2][1][1] != where[2][1][1]):
                                    return False # do not allow different having conditions
                                elif type(where[3]) == list and type(w[3]) == list:
                                    return False # do not allow two subquery
                            if type(where[3]) == list and  ( where[1] == 12 or (w_i>0 and self.sql['sql']['where'][w_i-1] == "except_" and where[1] == 10) ) and type(w[2][1][1]) == int:
                                bridge_table = schema.column_tokens_table_idx[where[3][1]] if schema.column_tokens_table_idx[where[3][1]] in schema.original_table["bridge_table"] else -1
                                if w not in to_be_replaced_component.sql['sql']['where'] and schema.column_tokens_table_idx[where[3][1]] == schema.column_tokens_table_idx[w[2][1][1]]:
                                    return False # new subquery conflict with original contion, such as: do not have any student and their name is xxx
                                if bridge_table != -1 and schema.column_tokens_table_idx[w[2][1][1]] in schema.original_table["many2many"][str(bridge_table)]:
                                    if len(select_tables) == 1:
                                        if (schema.column_tokens_table_idx[w[2][1][1]] not in select_tables and select_tables[0] in schema.original_table["many2many"][str(bridge_table)]):
                                            return False  # such as: what is name of student that do not have any student
                                    else:
                                        match = [ True if t in schema.original_table["many2many"][str(bridge_table)] else False for t in select_tables]
                                        if match.count(True) == 2 or (schema.column_tokens_table_idx[w[2][1][1]] not in select_tables and True in match):
                                            return False  # such as: what is name of student that do not have any student
                            if where[2][1][1] == w[2][1][1] and type(w[2][1][1]) == int and schema.column_types[where[2][1][1]] == 'boolean':
                                return False # boolean type conflict
            where_num = 0
            if type(self.sql['sql']['where'][0]) == list:
                where = self.sql['sql']['where'][0]
                for w in to_be_replaced_component.sql_or['sql']['where']:
                    if w not in to_be_replaced_component.sql['sql']['where']:
                        where_num += 1
                        if type(w) == list and  type(w[3]) == list:
                            if w[3][1] == where[2][1][1]:
                                return False # subquery conflict with following conditions
                            if to_be_replaced_component.sql_or['sq'].sub_sequence_toks[to_be_replaced_component.sq_idxs[0]][0] != self.sql_or['sq'].sub_sequence_toks[self.sq_idxs[0]][0]:
                                return False # subquery representation is not consistent with with following condition representation
            elif self.sql['sql']['where'][0] in ["except_","intersect_","union_"]:
                for w in to_be_replaced_component.sql_or['sql']['where']:
                    if w not in to_be_replaced_component.sql['sql']['where']:
                        where_num += 1
                        if type(w) == list:
                            return False # Can not add an IUE after an condition
            if where_num >= 5:
                return False # too complex

        if self.condition[4]:
            return False
        
        # Position match:
        if self.sql['sp_id'] != to_be_replaced_component.sql['sp_id'] and (self.sql['sp_id'] == 0 or to_be_replaced_component.sql['sp_id'] == 0):
            return False
        elif to_be_replaced_component.sql['sp_id'] != 0 and (to_be_replaced_component.sql['sp_id'] + 1 != to_be_replaced_component.sql['sp_num'] or self.sql['sp_id'] + 1 != self.sql['sp_num']):
            return False

        if to_be_replaced_component.sql_or['select_table_idxs'] != self.sql_or['select_table_idxs']:
            return False

        last_word_match = False
        if self.sql['question_range'][0] > 0 and to_be_replaced_component.sql['question_range'][0] > 0 and self.sql_or['question_lemma_toks'][self.sql['question_range'][0]-1] == to_be_replaced_component.sql_or['question_lemma_toks'][to_be_replaced_component.sql['question_range'][0]-1]:
            last_word_match = True

        if not last_word_match and (self.condition[0] or (to_be_replaced_component.sql_or['table_idxs'] != self.sql_or['table_idxs'])): # last word match
            return False
        elif not last_word_match:
            first_cond_idx = to_be_replaced_component.sq_idxs[0] - 1
            if first_cond_idx < 0:
                return False
            if self.sql_or['question_toks'][self.sql['question_range'][0]] not in ["in","on","with"]:
                if not last_word_match and self.sql_or['question_tag'][self.sql['question_range'][0]] != to_be_replaced_component.sql_or['sq'].sequence_tag[first_cond_idx][0]: 
                    if self.sql_or['question_tag'][self.sql['question_range'][0]][0] != to_be_replaced_component.sql_or['sq'].sequence_tag[first_cond_idx][0][0] or self.sql_or['question_tag'][self.sql['question_range'][0]][0] != 'V':
                        return False  # tag match failed
                if (self.sql_or['question_toks'][self.sql_or['sq'].offset[self.sq_idxs[0]]] in {"what","which","who","whom","whose","that","where","when","why"} or to_be_replaced_component.sql_or['question_toks'][to_be_replaced_component.sql_or['sq'].offset[first_cond_idx]] in {"what","which","who","whom","whose","that","where","when","why"}) and self.sql_or['question_toks'][self.sql_or['sq'].offset[self.sq_idxs[0]]] != to_be_replaced_component.sql_or['question_toks'][to_be_replaced_component.sql_or['sq'].offset[first_cond_idx]]:
                    return False # clause match failed

        # check groupBy
        if self.condition[1]:
            if self.condition[1] == to_be_replaced_component.sql['sql']['groupBy']:
                pass
            elif len(self.condition[1]) == 1 and len(to_be_replaced_component.sql_or['sql']['select'][1]) == 1 and not to_be_replaced_component.sql_or['sql']['select'][1][0][0] and to_be_replaced_component.sql_or['sql']['select'][1][0][1][1] == self.condition[1][0] and to_be_replaced_component.sql_or['sql']['select'] == self.sql_or['sql']['select']:
                pass
            else:
                return False

        # check orderBy
        if self.condition[3]:
            if self.sub_question.startswith("or ") or to_be_replaced_component.sub_question.startswith("or ") or (to_be_replaced_component.sq_idxs[0] - 1 >= 0 and to_be_replaced_component.sql_or['sq'].sub_sequence_toks[to_be_replaced_component.sq_idxs[0] - 1][-1] == "or") or to_be_replaced_component.sql_or['question_lemma_toks'][to_be_replaced_component.sql['question_range'][0]-1] == 'or':
                return False
            order_by_contain_agg = False
            order_by_in_select_columns = False
            order_by_in_select_tables = True
            order_by_cols = []
            for col in self.sql['sql']['orderBy'][1]:
                agg = col[0] if col[0] else col[1][0]
                if agg:
                    order_by_contain_agg = True
                order_by_cols.append([agg,col[1][1]])
            for select in to_be_replaced_component.sql_or['sql']['select'][1]:
                agg = select[0] if select[0] else select[1][1][0]
                if [agg,select[1][1][1]] in order_by_cols:
                    order_by_in_select_columns = True
            
            for col in self.sql['sql']['orderBy'][1]:
                if self.table['column_names'][col[1][1]][0] not in to_be_replaced_component.sql_or['select_table_idxs']:
                    order_by_in_select_tables = False
                    break
            
            if order_by_in_select_columns or (not order_by_contain_agg and not to_be_replaced_component.sql_or['agg_in_select'] and order_by_in_select_tables):
                pass
            else:
                return False

        if self.condition[3] or (to_be_replaced_component.sql_or['sql']['orderBy'] and not to_be_replaced_component.sql['sql']['orderBy']):
            orderBy_col = to_be_replaced_component.sql_or['sql']['orderBy'][1][0][1] if to_be_replaced_component.sql_or['sql']['orderBy'] else self.sql['sql']['orderBy'][1][0][1]
            where_num = 0
            for w in to_be_replaced_component.sql_or['sql']['where']:
                if w not in to_be_replaced_component.sql['sql']['where']:
                    where_num += 1
                    if type(w) == list and type(w[3]) == list:
                        return False
                    if type(w) == list and orderBy_col[0] and w[2][1][0] and schema.column_tokens_table_idx[orderBy_col[1]] != schema.column_tokens_table_idx[w[2][1][1]]:
                        return False #agg match failed
                if w in ["except_","intersect_","union_"]:
                    return False
            if where_num >= 5:
                return False
            for w_i,where in enumerate(self.sql['sql']['where']):
                if where in ["except_","intersect_","union_"]:
                    return False
                if type(where) == list and orderBy_col[0] and where[2][1][0] and schema.column_tokens_table_idx[orderBy_col[1]] != schema.column_tokens_table_idx[where[2][1][1]]:
                    return False #agg match failed
            if len(self.sql['sql']['where']) == 3 and self.sql['sql']['where'][0][2][1][1] == self.sql['sql']['where'][2][2][1][1] and type(self.sql['sql']['where'][0][2][1][1]) == int and self.sql['sql']['where'][0][1] == self.sql['sql']['where'][2][1]:
                return False
        return True



    def check_expand(self, sql, components_list, schema):
        if len(sql['sq'].sub_sequence_type) > 2: # prevent generating too complex examples
            if self.sq_idxs[0] == 0 or len(sql['sq'].sub_sequence_type) > 3:
                return False
        
        if len(self.sql['sql']['where']) > 3:  # prevent generating too complex examples
            return False

        if sql['sql']['orderBy'] and self.sql['sql']['orderBy']:
            return False # orderBy redundant
        
        if sql['sq'].sentence_num > 2:
            return False # original sentence is too complex

        where_num = 0
        negative_inside = False
        if sql['sql']['where']:
            for where in sql['sql']['where']:
                if type(where) == list:
                    if where[1] != 15:
                        where_num += 1
                    for w in self.sql['sql']['where']:
                        if type(w) == list and w[2][1][1] == where[2][1][1]:
                            return False
                if (type(where) == list and where[1] == 12) or where == 'except_':
                    negative_inside = True
            
            if self.sql['sql']['where'] and where_num > 1: 
                return False
            
            if self.sql['sql']['where'] and len(sql['sql']['where']) == 3 and (len(self.sql['sql']['where'])>1 or type(sql['sql']['where'][2][2][1][1]) != int or type(self.sql['sql']['where'][0][2][1][1]) != int or schema.column_tokens_table_idx[sql['sql']['where'][2][2][1][1]] != schema.column_tokens_table_idx[self.sql['sql']['where'][0][2][1][1]]):
                return False
            
            if self.sql['sql']['where'] and where_num == 0 and type(sql['sql']['where'][0][3]) == list and (len(self.sql['sql']['where'])>1 or type(self.sql['sql']['where'][0][2][1][1]) != int or schema.column_tokens_table_idx[sql['sql']['where'][0][3][1]] != schema.column_tokens_table_idx[self.sql['sql']['where'][0][2][1][1]]):
                return False

            if negative_inside:
                return False

            if self.sql['sql']['where']:
                select_tables = []
                agg_in_select = False
                for select in sql['sql']['select'][1]:
                    select_tables.append(schema.column_tokens_table_idx[select[1][1][1]])
                    if select[0] or select[1][1][0]:
                        agg_in_select = True
                select_tables = list(set(select_tables))

                if agg_in_select and sql['sql']['where'] and ((len(self.sql['sql']['where']) == 3 and self.sql['sql']['where'][0][2][1][1] == self.sql['sql']['where'][2][2][1][1]) or len(self.sql['sql']['where']) > 3):
                    return False # Prevention of complex IUE

                for w_i,where in enumerate(self.sql['sql']['where']):
                    if type(where) == list:
                        if where[2][1][0] and agg_in_select:
                            return False
                        for w in sql['sql']['where']:
                            if type(w) == list:
                                if type(where[3]) == list and  type(w[3]) == list:
                                    return False # do not allow two subquery
                                elif type(w[3]) == list:
                                    return False # do not allow subquery + new conditions
                                elif w[2][1][0] and where[2][1][0] and (where[2][1][0] != where[2][1][0] or where[2][1][1] != where[2][1][1]):
                                    return False # do not allow different having conditions
                                elif type(where[3]) == list and  ( where[1] == 12 or (w_i>0 and self.sql['sql']['where'][w_i-1] == "except_" and where[1] == 10) ) and type(w[2][1][1]) == int:
                                    bridge_table = schema.column_tokens_table_idx[where[3][1]] if schema.column_tokens_table_idx[where[3][1]] in schema.original_table["bridge_table"] else -1
                                    if schema.column_tokens_table_idx[where[3][1]] == schema.column_tokens_table_idx[w[2][1][1]]:
                                        return False # new subquery conflict with original contion, such as: do not have any student and their name is xxx
                                    if bridge_table != -1 and schema.column_tokens_table_idx[w[2][1][1]] in schema.original_table["many2many"][str(bridge_table)]:
                                        if len(select_tables) == 1:
                                            if schema.is_bridge_table(schema.column_tokens_table_idx[w[2][1][1]],select_tables[0],bridge_table) or schema.column_tokens_table_idx[w[2][1][1]] not in select_tables and select_tables[0] in schema.original_table["many2many"][str(bridge_table)]:
                                                return False  # such as: what is name of student that do not have any student
                                        else:
                                            match = [ True if t in schema.original_table["many2many"][str(bridge_table)] else False for t in select_tables]
                                            if match.count(True) == 2 or (schema.column_tokens_table_idx[w[2][1][1]] not in select_tables and True in match):
                                                return False  # such as: what is name of student that do not have any student
                                if where[2][1][1] == w[2][1][1] and type(w[2][1][1]) == int and schema.column_types[where[2][1][1]] == 'boolean':
                                    return False # boolean type conflict
        for w_i,where in enumerate(self.sql['sql']['where']):
            if type(where) == list and type(where[3]) == list and sql['sql']['orderBy']:
                return False # do not allow order following a new condtion
        condition_table_match = False
        last_word_match = False
        if self.sq_idxs[0] == 0:
            # add component in the begining
            if negative_inside or len(sql['sql']['where']) > 1 or sql['sq'].sentence_num != 1:
                return False
            if self.sql['question_toks'][self.sql['question_range'][1]] not in [".","?"]:
                return False
            if sql['question_lemma_toks'][0] not in ['list', 'find', 'return',  'show', 'give', 'tell']:
                return False
        else:
            # add component in the end
            if len(sql['sq'].sub_sequence_type) == 3 and not sql['sp_data'][-1]['sql']['where'] and not sql['sp_data'][-1]['sql']['orderBy']:
                return False

            # Word Match
            if self.condition[0] or (sql['table_idxs'] != self.sql_or['table_idxs']) or self.sql['sql']['where'] or self.sql['sql']['limit']: # last word match
                if self.sql['question_range'][0] > 0 and where_num == 0 and not sql['sql']['orderBy'] and (self.sql_or['question_lemma_toks'][self.sql['question_range'][0]-1] == sql['question_lemma_toks'][-1] or (self.sql_or['question_lemma_toks'][self.sql['question_range'][0]-1] == sql['question_lemma_toks'][-2] and sql['question_lemma_toks'][-1] in [".","?"])):
                    last_word_match = True
                else:
                    first_cond_idx = -1
                    if len(sql['sq'].sub_sequence_type) == 1 or (len(sql['sq'].sub_sequence_type) == 2 and sql['sq'].sentence_num == 2):
                        return False
                    else:
                        assert len(sql['sq'].sub_sequence_type) in [2,3]
                        if [sql['or_id'],[1,2]] in components_list:
                            for i in range(1,3):
                                for where in sql['sp_data'][i]['sql']['where']:
                                    if type(where) == list and (where[1] != 15 or (i==1 and sql['sp_data'][i+1]['sql']['where'] and type(sql['sp_data'][i+1]['sql']['where'][0])==list and type(sql['sp_data'][i+1]['sql']['where'][0][2][1][1]) == int and schema.column_tokens_table_idx[where[3][1]] == schema.column_tokens_table_idx[sql['sp_data'][i+1]['sql']['where'][0][2][1][1]])):
                                        first_cond_idx = i
                                if sql['sp_data'][i]['sql']['limit']:
                                    first_cond_idx = i
                                if first_cond_idx > 0:
                                    break
                        elif [sql['or_id'],[1]] in components_list:
                            for where in sql['sp_data'][1]['sql']['where']:
                                if type(where) == list and where[1] != 15:
                                    first_cond_idx = 1
                            if sql['sp_data'][1]['sql']['limit']:
                                first_cond_idx = 1
                        else:
                            return False # original sentence is too complex
                        if first_cond_idx == -1:
                            return False # none condtion find

                    if first_cond_idx != -1 and sql['sql']['orderBy'] and not sql['sql']['limit']:
                        return False # not allow order by + where
                    if sql['sq'].sub_sequence_toks[first_cond_idx-1][-1] == self.sql_or['question_lemma_toks'][self.sql['question_range'][0]-1] or sql['sq'].sub_sequence_lemma_toks[first_cond_idx-1][-1] == self.sql_or['question_lemma_toks'][self.sql['question_range'][0]-1]:
                        last_word_match = True
                    if not last_word_match and self.sql_or['question_tag'][self.sql['question_range'][0]] != sql['sq'].sequence_tag[first_cond_idx][0]: 
                        if self.sql_or['question_tag'][self.sql['question_range'][0]][0] != sql['sq'].sequence_tag[first_cond_idx][0][0] or self.sql_or['question_tag'][self.sql['question_range'][0]][0] != 'V':
                            return False  # tag match failed
                    if (self.sql_or['question_toks'][self.sql_or['sq'].offset[self.sq_idxs[0]]] in {"what","which","who","whom","whose","that","where","when","why"} or sql['question_toks'][sql['sq'].offset[first_cond_idx]] in {"what","which","who","whom","whose","that","where","when","why"}) and self.sql_or['question_toks'][self.sql_or['sq'].offset[self.sq_idxs[0]]] != sql['question_toks'][sql['sq'].offset[first_cond_idx]]:
                        return False # clause match failed
                    
                    if first_cond_idx != -1 and len(sql['sp_data'][first_cond_idx]['sql']['where']) == 1 and type(sql['sp_data'][first_cond_idx]['sql']['where'][0][2][1][1]) == int and len(self.sql['sql']['where']) == 1 and type(self.sql['sql']['where'][0][2][1][1]) == int  and schema.column_tokens_table_idx[sql['sp_data'][first_cond_idx]['sql']['where'][0][2][1][1]] ==  schema.column_tokens_table_idx[self.sql['sql']['where'][0][2][1][1]] and not self.sql['sql']['orderBy'] and not self.sql['sql']['limit']:
                        condition_table_match = True

                    

        ##################(Modified from replacement)##################
        if self.sub_question[1:-1].lower() in sql['question_or'].lower():
            return False # question similar
        
        if len(self.sql['sql']['where']) == 3 and self.sql['sql']['where'][0][2][1][1] == self.sql['sql']['where'][2][2][1][1] and type(self.sql['sql']['where'][0][2][1][1]) == int and schema.column_types[self.sql['sql']['where'][0][2][1][1]] == 'boolean':
            return False
        elif len(sql['sql']['where']) == 3 and sql['sql']['where'][0][2][1][1] == sql['sql']['where'][2][2][1][1] and type(sql['sql']['where'][0][2][1][1]) == int and schema.column_types[sql['sql']['where'][0][2][1][1]] == 'boolean':
            return False

        if self.condition[4]:
            return False

        if sql['select_table_idxs'] != self.sql_or['select_table_idxs'] and not condition_table_match:
            return False        

        # check groupBy 
        if self.condition[1]:
            if self.condition[1] == sql['sql']['groupBy']:
                pass
            elif len(self.condition[1]) == 1 and len(sql['sql']['select'][1]) == 1 and not sql['sql']['select'][1][0][0] and sql['sql']['select'][1][0][1][1] == self.condition[1][0] and sql['sql']['select'] == self.sql_or['sql']['select']:
                pass
            else:
                return False

        if sql['sql']['where'] and self.sql['sql']['limit'] and self.sql['sql']['orderBy']:
            for col in self.sql['sql']['orderBy'][1]:
                for w in sql['sql']['where']:
                    if type(w) == list and w[2][1][1] == col[1][1]:
                        return False
        elif self.sql['sql']['where'] and sql['sql']['limit'] and sql['sql']['orderBy']:
            for col in sql['sql']['orderBy'][1]:
                for w in self.sql['sql']['where']:
                    if type(w) == list and w[2][1][1] == col[1][1]:
                        return False

        # check orderBy 
        if self.condition[3]:
            if sql['sql']['groupBy']:
                return False
            order_by_contain_agg = False
            order_by_in_select_columns = False
            order_by_in_select_tables = True
            order_by_cols = []
            for col in self.sql['sql']['orderBy'][1]:
                agg = col[0] if col[0] else col[1][0]
                if agg:
                    order_by_contain_agg = True
                order_by_cols.append([agg,col[1][1]])
            for select in sql['sql']['select'][1]:
                agg = select[0] if select[0] else select[1][1][0]
                if [agg,select[1][1][1]] in order_by_cols:
                    order_by_in_select_columns = True
            
            for col in self.sql['sql']['orderBy'][1]:
                if self.table['column_names'][col[1][1]][0] not in sql['select_table_idxs']:
                    order_by_in_select_tables = False
                    break
            
            if order_by_in_select_columns or (not order_by_contain_agg and not sql['agg_in_select'] and order_by_in_select_tables):
                pass
            else:
                return False
            

        if self.condition[3] or sql['sql']['orderBy']: # (Modified from replacement)
            orderBy_col = sql['sql']['orderBy'][1][0][1] if sql['sql']['orderBy'] else self.sql['sql']['orderBy'][1][0][1]
            # subquery + order by
            for w in sql['sql']['where']:
                if type(w) == list:
                    if type(w[3]) == list:
                        return False
                    if orderBy_col[0] and w[2][1][0] and schema.column_tokens_table_idx[orderBy_col[1]] != schema.column_tokens_table_idx[w[2][1][1]]:
                        return False #agg match failed
                if w in ["except_","intersect_","union_"]:
                    return False
                
            if len(sql['sql']['where']) >= 5:
                return False
            for w_i,where in enumerate(self.sql['sql']['where']):
                if where in ["except_","intersect_","union_"]:
                    return False
                if type(where) == list and orderBy_col[0] and where[2][1][0] and schema.column_tokens_table_idx[orderBy_col[1]] != schema.column_tokens_table_idx[where[2][1][1]]:
                    return False #agg match failed
            if len(self.sql['sql']['where']) == 3 and self.sql['sql']['where'][0][2][1][1] == self.sql['sql']['where'][2][2][1][1] and type(self.sql['sql']['where'][0][2][1][1]) == int and self.sql['sql']['where'][0][1] == self.sql['sql']['where'][2][1]:
                return False

        if where_num and self.sq_idxs[0] != 0 and self.sql['sql']['where'] and len(self.sql['sql']['where']) <= 2 and not self.sql['sql']['orderBy'] and not self.sql['sql']['limit']:
            return 2
        if self.sql['sql']['orderBy'] and not self.sql['sql']['limit']:
            return 3
        return 1


    def replace_component(self, to_be_replaced_component, schema):
        # generate new split data
        old_sp_ids = []
        for sql_sp in to_be_replaced_component.sql_sps:
            old_sp_ids.append(sql_sp['sp_id'])
        new_sp_data = []
        only_once = True
        sq = copy.deepcopy(self.sql_or['sq'])
        sq.clean_data()
        for i,sql_sp in enumerate(to_be_replaced_component.sql_or['sp_data']):
            if sql_sp['sp_id'] in old_sp_ids:
                if only_once:
                    only_once = False
                    #Strat replace:
                    for sql_sp2 in self.sql_sps:
                        new_sp_data.append(copy.deepcopy(sql_sp2))
                        sq.add_sub_element(self.sql_or['sq'], sql_sp2['sp_id'] )
            else:
                if i>0 and not only_once and len(new_sp_data) != len(self.sql_sps):
                    new_sp_data.insert(-1,copy.deepcopy(to_be_replaced_component.sql_or['sp_data'][i-1]))
                    sq.add_sub_element(to_be_replaced_component.sql_or['sq'], i-1 , insert_idx = -1)
                    break
                else:
                    new_sp_data.append(copy.deepcopy(sql_sp))
                    sq.add_sub_element(to_be_replaced_component.sql_or['sq'], i )
       
        question = ""
        last_question_range = []
        for i,sql_sp in enumerate(new_sp_data):
            sql_sp['sp_num'] = len(new_sp_data)
            sql_sp['sp_id'] = i
            question += " " + sql_sp['sub_question']
            if i > 0:
                sql_sp['question_range'] = [last_question_range[1]+1,last_question_range[1]+1+sql_sp['question_range'][1] - sql_sp['question_range'][0]]
            else:
                assert sql_sp['question_range'][0] == 0
            last_question_range = sql_sp['question_range']
        for i,sql_sp in enumerate(new_sp_data):
            if 'or_data' in sql_sp:
                sql_sp.pop('or_data')
            sql_sp['question'] = question[1:]
            sql_sp['question_toks'] = sql_sp['question'].split(" ")
        assert new_sp_data[-1]['question_range'][1] + 1 == len(sql_sp['question_toks'])
        assert " ".join(sq.sub_sequence_list)  == question[1:]
        sq.question_lemma = " ".join([t for lt in sq.sub_sequence_lemma_toks for t in lt])
        sq.question_or = question[1:]
        assert sq.question_lemma.count(" ") == question[1:].count(" ")

        # combine new splite data into a new sql_or obj
        # 1. Generate new NatSQL
        # 2. Generate SQL and parsed this NatSQL
        sp_sql_only = []
        for i,sql in enumerate(new_sp_data):
            sql['sql']['db_id'] = sql['db_id']
            sql['sql']['score'] = 0
            if sql['sql']['union'] and type(sql['sql']['union']) == list:
                empty_sql =  {'limit':None,'intersect':None,'union':None,'except':None,'having':[],'orderBy':[],'groupBy':[],'where':[],"select":[False,[]]}
                empty_sql["select"] = [False,[sql['sql']['union']]]
                sql['sql']['union'] = empty_sql
            if sql['sql']['intersect'] and type(sql['sql']['intersect']) == list:
                empty_sql =  {'limit':None,'intersect':None,'union':None,'except':None,'having':[],'orderBy':[],'groupBy':[],'where':[],"select":[False,[]]}
                empty_sql["select"] = [False,[sql['sql']['intersect']]]
                sql['sql']['intersect'] = empty_sql
            sp_sql_only.append([sql['sql']])
            # sql['sql'] = [sql['sql']]
        beam_idxs = [0] * len(new_sp_data) 
        global database_path
        final_sql, beam_idxs, natsql, p_nsql = generate_natsql_from_split_data(sp_sql_only,sq,schema,beam_idxs,database_path,False)
        return final_sql,natsql,question[1:],[[i] * len(sslt) for i,sslt in enumerate(sq.sub_sequence_lemma_toks)]


    def append_component(self, sql, append_type, schema):
        # generate new split data
        new_sp_data = []
        for sql_sp in sql['sp_data']:
            new_sp_data.append(copy.deepcopy(sql_sp))
        where_num = 0
        for where in sql['sql']['where']:
            if type(where) == list:
                if where[1] != 15:
                    where_num += 1

        sq = copy.deepcopy(sql['sq'])
        if self.sq_idxs[0] == 0:
            for sql_sp in reversed(self.sql_sps):
                new_sp_data.insert(0,copy.deepcopy(sql_sp))
                sq.add_sub_element(self.sql_or['sq'], sql_sp['sp_id'], 0 )
        else: # append in the end
            if new_sp_data[-1]['question_toks'][new_sp_data[-1]['question_range'][1]] in ["?","."]:
                if where_num or sql['sp_data'][-1]['sql']['limit']:
                    if append_type == 2:
                        new_sp_data[-1]['question_toks'][new_sp_data[-1]['question_range'][1]] = "or"
                        sq.sub_sequence_lemma_toks[-1][-1] = "or"
                        sq.sub_sequence_toks[-1][-1] = "or"
                        sq.pattern_tok[-1][-1] = "or"
                    elif append_type == 3:
                        new_sp_data[-1]['question_toks'][new_sp_data[-1]['question_range'][1]] = ","
                        sq.sub_sequence_lemma_toks[-1][-1] = ","
                        sq.sub_sequence_toks[-1][-1] = ","
                        sq.pattern_tok[-1][-1] = ","
                    else:
                        new_sp_data[-1]['question_toks'][new_sp_data[-1]['question_range'][1]] = "and"
                        sq.sub_sequence_lemma_toks[-1][-1] = "and"
                        sq.sub_sequence_toks[-1][-1] = "and"
                        sq.pattern_tok[-1][-1] = "and"
                else:
                    if append_type == 1 and self.sql_or['question_toks'][self.sql_or['sq'].offset[self.sql_sps[0]['sp_id']]] not in {"what","which","who","whom","whose","that","where","when","why"} and self.sql_or['question_tag'][self.sql_or['sq'].offset[self.sql_sps[0]['sp_id']]][0] == "V":
                        new_sp_data[-1]['question_toks'][new_sp_data[-1]['question_range'][1]] = "that"
                        sq.sub_sequence_lemma_toks[-1][-1] = "that"
                        sq.sub_sequence_toks[-1][-1] = "that"
                        sq.pattern_tok[-1][-1] = "that"
                    else:
                        new_sp_data[-1]['question_toks'][new_sp_data[-1]['question_range'][1]] = ","
                        sq.sub_sequence_lemma_toks[-1][-1] = ","
                        sq.sub_sequence_toks[-1][-1] = ","
                        sq.pattern_tok[-1][-1] = ","
            else:
                if where_num:
                    if append_type == 2:
                        new_sp_data[-1]['question_toks'].append("or")
                        sq.sub_sequence_lemma_toks[-1].append("or")
                        sq.sub_sequence_toks[-1].append("or")
                        sq.pattern_tok[-1].append("or")
                    elif append_type == 3:
                        new_sp_data[-1]['question_toks'].append(",")
                        sq.sub_sequence_lemma_toks[-1].append(",")
                        sq.sub_sequence_toks[-1].append(",")
                        sq.pattern_tok[-1].append(",")
                    else:
                        new_sp_data[-1]['question_toks'].append("and")
                        sq.sub_sequence_lemma_toks[-1].append("and")
                        sq.sub_sequence_toks[-1].append("and")
                        sq.pattern_tok[-1].append("and")
                else:
                    if append_type == 1 and self.sql_or['question_toks'][self.sql_or['sq'].offset[self.sql_sps[0]['sp_id']]] not in {"what","which","who","whom","whose","that","where","when","why"} and self.sql_or['question_tag'][self.sql_or['sq'].offset[self.sql_sps[0]['sp_id']]][0] == "V":
                        new_sp_data[-1]['question_toks'].append("that")
                        sq.sub_sequence_lemma_toks[-1].append("that")
                        sq.sub_sequence_toks[-1].append("that")
                        sq.pattern_tok[-1].append("that")
                    else:
                        new_sp_data[-1]['question_toks'].append(",")
                        sq.sub_sequence_lemma_toks[-1].append(",")
                        sq.sub_sequence_toks[-1].append(",")
                        sq.pattern_tok[-1].append(",")
                new_sp_data[-1]['question_range'][1] += 1
                sq.col_match[-1].append([])
                sq.db_match[-1].append([])
                sq.full_db_match[-1].append([])
                sq.idx2sub_id.append([sq.idx2sub_id[-1][0],sq.idx2sub_id[-1][1]+1])
                sq.original_idx[-1].append(sq.original_idx[-1][-1]+1)
                sq.sequence_entt[-1].append('')
                sq.sequence_tag[-1].append('')
                sq.table_match[-1].append([])
                sq.table_match_weight[-1].append([])

            new_sp_data[-1]['sub_question'] = " ".join([new_sp_data[-1]['question_toks'][i] for i in range(new_sp_data[-1]['question_range'][0], new_sp_data[-1]['question_range'][1]+1)])
            sq.sub_sequence_list[-1] = new_sp_data[-1]['sub_question']
            for sql_sp in self.sql_sps:
                new_sp_data.append(copy.deepcopy(sql_sp))
                sq.add_sub_element(self.sql_or['sq'], sql_sp['sp_id'] )
        
        sq.question_or = " ".join(sq.sub_sequence_list)
        sq.question_lemma = " ".join([ " ".join(sslt) for sslt in sq.sub_sequence_lemma_toks ])
        
        # combine new splite data into a new sql_or obj
        # 1. Generate new NatSQL
        # 2. Generate SQL and parsed this NatSQL
        sp_sql_only = []
        for i,sql in enumerate(new_sp_data):
            sql['sql']['db_id'] = sql['db_id']
            sql['sql']['score'] = 0
            if sql['sql']['union'] and type(sql['sql']['union']) == list:
                empty_sql =  {'limit':None,'intersect':None,'union':None,'except':None,'having':[],'orderBy':[],'groupBy':[],'where':[],"select":[False,[]]}
                empty_sql["select"] = [False,[sql['sql']['union']]]
                sql['sql']['union'] = empty_sql
            if sql['sql']['intersect'] and type(sql['sql']['intersect']) == list:
                empty_sql =  {'limit':None,'intersect':None,'union':None,'except':None,'having':[],'orderBy':[],'groupBy':[],'where':[],"select":[False,[]]}
                empty_sql["select"] = [False,[sql['sql']['intersect']]]
                sql['sql']['intersect'] = empty_sql
            sp_sql_only.append([sql['sql']])
            # sql['sql'] = [sql['sql']]
        beam_idxs = [0] * len(new_sp_data) 
        global database_path
        final_sql, beam_idxs, natsql, p_nsql = generate_natsql_from_split_data(sp_sql_only,sq,schema,beam_idxs,database_path,False)
        sq.question_or = sq.question_or.replace(" , , ", " , ")
        if sq.question_or[-1].isalpha():
            sq.question_or = sq.question_or + " ?"
        
        return final_sql,natsql,sq.question_or,[[i] * len(sslt) for i,sslt in enumerate(sq.sub_sequence_lemma_toks)]



def construct_hyper_param():
    parser = argparse.ArgumentParser()
    parser.add_argument('--spiderSS_preprocessed_file', default='', type=str)
    parser.add_argument('--spiderSS_for_models', default='', type=str)
    parser.add_argument('--database', default='', type=str)
    parser.add_argument('--natsql_table', default='NatSQLv1_6/tables_for_natsql.json', type=str)
    parser.add_argument('--orgin_table', default='NatSQL/NatSQLv1_6/tables.json', type=str)
    parser.add_argument('--CG_type', default='substitute', type=str)
    parser.add_argument('--spiderCG_out_file', default='', type=str)
    args = parser.parse_args()
    return args



def gennerate_compositional_examples(args):
    def dose_generate_workable_sql(parsed_sql,final_sql):
        if " * in" in final_sql or " * not in" in final_sql:
            return False
        agg_in_select = False
        for select in parsed_sql['select'][1]:
            if select[0] or select[1][1][0]:
                agg_in_select = True
                break
        if agg_in_select and (parsed_sql['intersect'] or parsed_sql['union'] or parsed_sql['except']):
            return False
        if parsed_sql['select'][1][0][1][1][1] == -1 and (parsed_sql['intersect'] or parsed_sql['union'] or parsed_sql['except']):
            if parsed_sql['union'] and parsed_sql['from'] != parsed_sql['union']['from']:
                return False
            if parsed_sql['intersect'] and parsed_sql['from'] != parsed_sql['intersect']['from']:
                return False
            if parsed_sql['except'] and parsed_sql['from'] != parsed_sql['except']['from']:
                return False
        return True
    tables = json.load(open(args.natsql_table, 'r'))
    all_tables = {}
    all_schema = {}
    for t in tables:
        all_tables[t['db_id']] = t
    orgin_tables = json.load(open(args.orgin_table, 'r'))
    all_orgin_tables = {}
    for t in orgin_tables:
        all_orgin_tables[t['db_id']] = t

    all_sql_or = dict()
    sqls = json.load(open(args.spiderSS_preprocessed_file,"r"))
    sql_sps = json.load(open(args.spiderSS_for_models,"r"))
    
    # preprocess sqls:
    for sql in sqls:
        sql['question_lemma_toks'] = sql['question_lemma'].split(" ")
        assert len(sql['question_lemma_toks']) == len(sql['question_toks'])
        if sql["db_id"] not in all_sql_or:
            all_sql_or[sql["db_id"]] = [sql]
        else:
            all_sql_or[sql["db_id"]].append(sql)

        table = all_tables[sql['db_id']]
        sql["agg_in_select"] = False
        sql["select_table_idxs"] = set()
        for select in sql['sql']['select'][1]:
            if select[0] or select[1][1][0]:
                sql["agg_in_select"] = True
            sql["select_table_idxs"].add(table['column_names'][select[1][1][1]][0])
        sql["table_idxs"] = copy.deepcopy(sql["select_table_idxs"])
        sub_query_in_sql = False
        if sql['sql']['where']:
            for where in sql['sql']['where']:
                if where in ["except" or "except_"]:
                    break
                if type(where) == list and type(where[2][1][1]) == int:
                    sql["table_idxs"].add(table['column_names'][where[2][1][1]][0])
                if type(where) == list and type(where[3]) == list:
                    sub_query_in_sql = True
                    break
        if sql['sql']['orderBy'] and not sub_query_in_sql:
            for col in sql['sql']['orderBy'][1]:
                sql["table_idxs"].add(table['column_names'][col[1][1]][0])
        sql["sp_data"] = []
        sql["sq"] = SubQuestion(sql["question"],sql["question_type"],sql["table_match"],sql["question_tag"],sql["question_dep"],sql["question_entt"],sql,run_special_replace=False)

    split_as_db = dict()
    for i,sql in enumerate(sql_sps):
        if sql["db_id"] not in split_as_db:
            split_as_db[sql["db_id"]] = [sql]
        else:
            split_as_db[sql["db_id"]].append(sql)
        sqls[sql['or_id']]["sp_data"].append(sql)
    new_data_set_through_add = []
    new_data_set_through_replace = []

    for d_i, db_id in enumerate(split_as_db):
        if db_id not in all_schema:
            all_schema[db_id] = Schema_Token(get_spacy_tokenizer(), None, all_tables[db_id], None)
            all_schema[db_id].add_lower_data(all_tables[db_id])

        components = []
        components_used = []
        last_sql = None
        for i, sql in enumerate(split_as_db[db_id]):
            sql_or = sqls[sql['or_id']]
            match_link = sql_or['match_link'][sql['sp_id']]
            if (sql['sp_id'] == 0 or (not last_sql['sql']['extra'] and (not last_sql['sql']['where'] or not (len(last_sql['sql']['where'])==1 and type(last_sql['sql']['where']) == list and last_sql['sql']['where'][0][1] == 15) )) ) and not sql['sql']['select'][1] and not sql['sql']['extra'] and (sql['sql']['where'] or sql['sql']['orderBy']):
                if -1 not in match_link and len(sql['sql']['where'])==1 and type(sql['sql']['where']) == list and sql['sql']['where'][0][1] == 15 and sql['sp_id'] + 1 < sql['sp_num'] and sql['sp_id'] == 0 and sql['question_toks'][sql['question_range'][1]] not in [".","?"] and split_as_db[db_id][i+1]['sql']['where'] and not split_as_db[db_id][i+1]['sql']['select'][1] and not split_as_db[db_id][i+1]['sql']['extra'] and not split_as_db[db_id][i+1]['sql']['groupBy'] and not split_as_db[db_id][i+1]['sql']['orderBy'] and sql['question_toks'][split_as_db[db_id][i+1]['question_range'][1]] in [".","?"] :
                    components.append(component(sql_or,[sql,split_as_db[db_id][i+1]],all_tables[db_id],[sql['sp_id'],sql['sp_id']+1]))
                    components_used.append([sql_or['or_id'],[sql['sp_id'],sql['sp_id']+1]])
                elif -1 not in match_link and len(sql['sql']['where'])==1 and type(sql['sql']['where']) == list and sql['sql']['where'][0][1] == 15 and sql['sp_id'] + 1 < sql['sp_num'] and sql['sp_id'] + 2 == sql['sp_num'] and split_as_db[db_id][i+1]['sql']['where'] and not split_as_db[db_id][i+1]['sql']['select'][1] and not split_as_db[db_id][i+1]['sql']['extra'] and not split_as_db[db_id][i+1]['sql']['groupBy'] and not split_as_db[db_id][i+1]['sql']['orderBy']:
                    components.append(component(sql_or,[sql,split_as_db[db_id][i+1]],all_tables[db_id],[sql['sp_id'],sql['sp_id']+1]))
                    components_used.append([sql_or['or_id'],[sql['sp_id'],sql['sp_id']+1]])
                elif -1 not in last_match_link and len(sql['sql']['where'])==1 and type(sql['sql']['where']) == list and sql['sql']['where'][0][1] in [2,7,9] and sql['sp_id'] + 1 == sql['sp_num'] and sql['sp_id'] > 1 and last_sql['sql']['where'] and len(last_sql['sql']['where']) == 1 and type(last_sql['sql']['where']) == list and last_sql['sql']['where'][0][1] == sql['sql']['where'][0][1] and last_sql['sql']['where'][0][2][1][1] == sql['sql']['where'][0][2][1][1] and not last_sql['sql']['select'][1] and not last_sql['sql']['extra'] and not last_sql['sql']['groupBy'] and not last_sql['sql']['orderBy'] :
                    components.append(component(sql_or,[last_sql,sql],all_tables[db_id],[sql['sp_id']-1,sql['sp_id']]))
                    components_used.append([sql_or['or_id'],[sql['sp_id']-1,sql['sp_id']]])
                elif len(sql['sql']['where'])==1 and type(sql['sql']['where']) == list and sql['sql']['where'][0][1] == 15:
                    pass
                elif -1 not in match_link:
                    components.append(component(sql_or,[sql],all_tables[db_id],[sql['sp_id']]))
                    components_used.append([sql_or['or_id'],[sql['sp_id']]])
                
            last_sql = sql
            last_match_link = match_link


        schema = Schema_Num(get_schema(os.path.join(args.database, db_id, db_id + ".sqlite")),all_orgin_tables[db_id])

        if args.CG_type == "substitute":
            for i11,cp1 in enumerate(components):
                for i22,cp2 in enumerate(components):
                    if cp1.sql_or['or_id'] !=  cp2.sql_or['or_id'] and cp1.sql['sql'] !=  cp2.sql['sql']:
                        if cp2.check_replacement(cp1,all_schema[db_id]):
                            final_sql,natsql,question,split_for_eval = cp2.replace_component(cp1,all_schema[db_id])
                            parsed_sql = get_original_sql(schema, final_sql)
                            if dose_generate_workable_sql(parsed_sql,final_sql):
                                new_data_set_through_replace.append({"db_id":db_id,"query":final_sql,"NatSQL":natsql,"question":question,"question_toks":question.split(" "),"sql":parsed_sql,"sp_for_eval":split_for_eval})
                                print(question)
                                print(natsql)
                                print(final_sql)
                                print(len(new_data_set_through_replace))
                                print()
        else:
            for cp1 in components:
                for sql in all_sql_or[db_id]:
                    if cp1.sql_or['or_id'] !=  sql['or_id']:
                        append_type = cp1.check_expand(sql, components_used, all_schema[db_id])
                        if append_type:
                            final_sql,natsql,question,split_for_eval = cp1.append_component(sql, 1 if append_type == 2 else append_type, all_schema[db_id])
                            if final_sql:
                                parsed_sql = get_original_sql(schema, final_sql)
                                if dose_generate_workable_sql(parsed_sql,final_sql):
                                    new_data_set_through_replace.append({"db_id":db_id,"query":final_sql,"NatSQL":natsql,"question":question,"question_toks":question.split(" "),"sql":parsed_sql,"sp_for_eval":split_for_eval})
                                    print(question)
                                    print(natsql)
                                    print(final_sql)
                                    print(len(new_data_set_through_replace))
                                if len(new_data_set_through_replace) == 3863:
                                    print()
                                if append_type == 2:
                                    final_sql,natsql,question,split_for_eval = cp1.append_component(sql, append_type, all_schema[db_id])
                                    parsed_sql = get_original_sql(schema, final_sql)
                                    if dose_generate_workable_sql(parsed_sql,final_sql):
                                        new_data_set_through_replace.append({"db_id":db_id,"query":final_sql,"NatSQL":natsql,"question":question,"question_toks":question.split(" "),"sql":parsed_sql,"sp_for_eval":split_for_eval})
                                        print(question)
                                        print(natsql)
                                        print(final_sql)
                                        print(len(new_data_set_through_replace))
                            if len(new_data_set_through_replace) == 3863:
                                print()
                            
    print(len(new_data_set_through_replace))
    json.dump(new_data_set_through_replace,open(args.spiderCG_out_file,'w'), indent=2)

database_path = None

if __name__ == '__main__':
    args = construct_hyper_param()
    database_path = args.database
    gennerate_compositional_examples(args)