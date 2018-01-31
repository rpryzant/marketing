""" 
extracts a category from the data
"""

import map_reduce
from collections import defaultdict


# make parent dicts
genre_tsv = '/Volumes/datasets/rakuten_dump/merchandize_data/genres/genres_with_english.tsv'
id_to_pid = {}
for l in open(genre_tsv):
    l = l.strip()
    try:
        [id, ja_name, pid, en_name] = l.split('\t')
    except:
        continue
    id_to_pid[id] = pid


# the category you want to extract
# from fashion_categories
CATEGORIES_TO_EXTRACT = [
    '502456',   '100454',    '502427',    '100472',    '110933',    '502368',    '100939',    '100433',    '216129',
    '550009',    '111103',    '207733',    '205193',    '551648',    '551648',    '206587',    '303816',    '206591',
    '206590',    '303803',    '551441',    '551409',    '551433',    '551403',    '551445',    '551413',    '551682',
    '551668',    '551648',    '551664',    '551644',    '551677',    '551672',    '551652',    '205197',    '200807',
    '207699',    '100542',    '100371',    '558929',    '204994',    '402513'    '402517',    '402515',    '508925',
    '508930',    '501642',    '402087',    '201780',    '302242',    '204982',    '201794',    '302464',    '407933',
    '502027',    '402463',    '402475',    '501965',    '501962',    '501963',    '501976', '506410', '200859'
]

def is_child(id):
    # is id the child of CATEGORY_TO_EXTRACT?
    while id in id_to_pid:
        if id in CATEGORIES_TO_EXTRACT:
            return True
        id = id_to_pid[id]
    return False


def map_fn(path):
    out = open(path + '.OUT', 'a')
    for l in open(path):
        parts = l.strip().split("\t")
        genre_id = parts[-1]
        if is_child(genre_id):
            out.write(l.strip() + '\n')
    return


def reduce_fn(result_list):
    return ''


map_reduce.mapreduce(
    map_fn=map_fn,
    reduce_fn=reduce_fn,
#    input_re='/scr/rpryzant/marketing/rakuten/data/products_tsv/*.tsv',
    input_re='/Volumes/datasets/rakuten_dump/merchandize_data/products_tsv/*.tsv',
    output_filename='/scr/rpryzant/marketing/rakuten/categories',
    n_cores=2

)
