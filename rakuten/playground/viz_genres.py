import sys
import random
from collections import defaultdict
import os
from tqdm import tqdm

products_tsv = '/Users/rpryzant/Documents/rakuten_data/clothing_products.tok'
genre_tsv = '/Users/rpryzant/Documents/rakuten_data/genres_with_english.tsv' 
out_dir = '~/Desktop/test'

# get genre graph
print 'BUILDING GRAPH...'
id_to_name = {}
id_to_pid = {}
pid_to_id = defaultdict(list)
for l in open(genre_tsv):
    l = l.strip()
    try:
        [id, ja_name, pid, en_name] = l.split('\t')
    except:
        continue

    id_to_name[id] = ja_name, en_name
    id_to_pid[id] = pid
    pid_to_id[pid].append(id)

# get counts
print 'PARSING DATA AND SUBSETTING GRAPH...'
id_to_count = defaultdict(int)
graph_gids = set()
for l in tqdm(open(products_tsv), total=20000000):
    genre_id = l.strip().split('\t')[-1]
    id_to_count[genre_id] += 1

    while genre_id in id_to_pid:
        graph_gids.add(genre_id)
        genre_id = id_to_pid[genre_id]
    graph_gids.add(genre_id)


# final counts
print 'GETTING FINAL COUNTS...'
final_counts = defaultdict(int)

def aggregate_counts(id):
    # total count of all descriptions for a genre
    out = id_to_count[id]
    for cid in pid_to_id[id]:
        if cid in final_counts:
            out += final_counts[cid]
        else:
            out += aggregate_counts(cid[:])
    return out

for gid in tqdm(graph_gids):
    final_counts[gid] = aggregate_counts(gid)



def dfs(path, id):
    if id not in graph_gids:
        return
    ja_name, en_name = id_to_name.get(id, ('unk', 'unk'))
    count = final_counts[id]
    dir_name = en_name.replace(" ", '_') + '|' + ja_name + '|' + id + '|' + str(count)
    new_path = os.path.join(path, dir_name)
    os.makedirs(new_path)
    for child_id in pid_to_id[id]:
        dfs(new_path, child_id)
        


print 'WRITING GRAPH...'
dfs('/Users/rpryzant/Desktop/test', '0')
