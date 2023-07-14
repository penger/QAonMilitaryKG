import time

import jieba
import jieba.posseg as pseg
import json
from elasticsearch import Elasticsearch, helpers

# 创建 Elasticsearch 客户端
es = Elasticsearch(
    ['http://192.168.1.123:9200/'],
    basic_auth=('elastic', 'elastic')
)

index_name = 'military_information'


# 写入数据到ES中
def insert_record2es():
    docs = []
    for record in open('data\military.json', 'r', encoding='utf-8'):
        data = json.loads(record)
        del data['_id']
        # 清洗最大起飞重量，去掉 千克标记
        if '最大起飞重量' in data:
            data['最大起飞重量'] = float(data['最大起飞重量'].replace("千克", ""))
            print(data)
        json_utf8 = json.loads(json.dumps(data).encode('utf-8'))
        print(json_utf8)
        doc = {"_index": index_name, "_source": json_utf8}
        docs.append(doc)
        if len(docs) % 100 == 0:
            start = time.time()
            helpers.bulk(es, docs)
            end = time.time()
            print(f"写入100条： 耗时 {end - start} 秒 ---- 每批次之后休息 3秒 ")
            docs.clear()
    helpers.bulk(es, docs)


# 自定义jieba分词器
def load_user_dict():
    for record in open('data\military.dict', 'r', encoding='utf-8'):
        line = record.split(" ")
        jieba.add_word(line[0], freq=3000, tag=line[2].strip())


def analysis_question(q: str):
    parser_dict = {}
    selected_tags = ['n_attr', 'n_time', 'n_big', 'n_small', 'n_unit', 'n_country', 'n_compare',
                     'n_most', 'n_weapon']
    wds = [(i.word, i.flag) for i in pseg.cut(q)]
    # print(wds)
    parser_dict['n_attrs'] = [wd for wd, flag in wds if flag == 'n_attr']
    parser_dict['n_times'] = [wd for wd, flag in wds if flag == 'n_time']
    parser_dict['n_bigs'] = [wd for wd, flag in wds if flag == 'n_big']
    parser_dict['n_smalls'] = [wd for wd, flag in wds if flag == 'n_small']
    parser_dict['n_countries'] = [wd for wd, flag in wds if flag == 'n_country']
    parser_dict['n_compares'] = [wd for wd, flag in wds if flag == 'n_compare']
    parser_dict['n_mosts'] = [wd for wd, flag in wds if flag == 'n_most']
    parser_dict['n_units'] = [wd for wd, flag in wds if flag == 'n_unit']
    parser_dict['n_weapons'] = [wd for wd, flag in wds if flag == 'n_weapon']
    parser_dict['pattern'] = [flag for wd, flag in wds if
                              flag in selected_tags]
    return parser_dict


def search_answer(parse_dict: dict):
    # print('step 1: 问句解析： ', parse_dict)
    pattern = parse_dict['pattern']
    # print('step 2: 查询模版：', pattern)
    search_data = []
    targets = ['名称']
    search_flag = 1
    # 国家
    country = parse_dict.get('n_countries')
    # 大类
    n_big = parse_dict.get('n_bigs')
    # 小类
    n_small = parse_dict.get('n_smalls')
    # 武器名称
    n_weapon = parse_dict.get('n_weapons')
    # 查询属性
    n_attr = parse_dict.get('n_attrs')
    # 极限值
    n_most = parse_dict.get('n_mosts')

    query_json, targets, sorts = [], [], []
    if country:
        query_json.append({"terms": {"产国.keyword": country}})
    if n_small:
        query_json.append({"terms": {"类型.keyword": n_small}})
    if n_big:
        query_json.append({"terms": {"大类.keyword": n_big}})
    if n_weapon:
        query_json.append({"match_phrase": {"名称": n_weapon[0]}})
    if n_attr:
        targets.append("名称")
        for item in n_attr:
            targets.append(item)
    else:
        targets = ["名称", "简介"]

    if n_most:
        if '轻' in n_most[0] or '小' in n_most[0]:
            sorts = [{"最大起飞重量": {"order": "asc"}}]
        else:
            sorts = [{"最大起飞重量": {"order": "desc"}}]
    return query_json, targets, sorts


def search_es(search_conditions: list, target: list, orders: list):
    # 遍历 conditions 生成filter 和must的组合
    must_json = [i for i in search_conditions if 'match_phrase' in i]
    filter_json = [i for i in search_conditions if 'term' in i or 'terms' in i]

    query = {"bool": {"must": must_json, "filter": filter_json}}

    r = es.search(index=index_name, size=3, source=target, query=query, sort=orders)

    # print("query is : ", query)
    elements = r['hits']['hits']
    total = r['hits']['total']['value']
    print("共查询到", total, "条记录：")
    for element in elements:
        for item in target:
            if item in element['_source']:
                print(item, " ：", element['_source'][item])
            else:
                print("未找到对应的", item)
        print("--------------------------------")


if __name__ == '__main__':
    # insert_record2es()

    print("加载用户词典 用于句子解析")
    load_user_dict()
    # question = '给我介绍一下中国的FC-1“枭龙”/JF-17“雷电”多用途攻击机'
    # question = '给我介绍一下日本航空母舰的编制,满载排水量,航速'
    # question = '给我介绍一下日本航空母舰的编制,满载排水量，航速,制造厂，服役时间，建造时间'
    # question = '辽宁舰的编制,满载排水量，航速,制造厂，服役时间，建造时间'
    # question = '辽宁舰介绍一下'
    # question = '最大起飞重量最小的是什么'
    # question = '美国最大起飞重量最大的是什么'
    # question = '美国最大起飞重量最大的无人机是什么'
    # question = '中国最大起飞重量最大的无人机的研发单位和发动机还有产国还有类型加上相应的图片'
    question = '长空-1无人机的发动机，气动布局是什么'
    question_parser = analysis_question(question)
    # print(question_parser)
    query_json, target_list, sorts = search_answer(question_parser)
    search_es(query_json, target_list, sorts)
