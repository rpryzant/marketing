"""
visualize the categories in a rowfile with a nested directory structure, 
where each directory is named
  english_name|japanese_name|id|num_descriptions
"""
from collections import defaultdict
from tqdm import tqdm
import os

genre_file = '/Users/rpryzant/Documents/rakuten_data/genres_with_english.tsv'
descriptions_file = '/Users/rpryzant/Documents/rakuten_data/clothing_products'
out_root = '/Users/rpryzant/Desktop/test'


print 'PARSING GENRE FILE...'
id_to_name = {}
id_to_pid = {}
pid_to_id = defaultdict(list)
for l in tqdm(open(genre_file)):
    l = l.strip()
    try:      [id, ja_name, pid, en_name] = l.split('\t')
    except:   continue
    id_to_name[id] = ja_name, en_name
    id_to_pid[id] = pid
    pid_to_id[pid].append(id)

print 'COUNTING GENRES...'
if os.path.exists('counts') and os.path.exists('total_counts'):
    counts = eval(open('counts').read().replace("<type 'int'>", 'int'))
    total_counts = eval(open('total_counts').read().replace("<type 'int'>", 'int'))
else:
    counts = defaultdict(int)
    for l in tqdm(open(descriptions_file), total=20000000):
        id = l.strip().split('\t')[-1]
        counts[id] += 1

    print 'SUMMING UP GENRE COUNTS....'
    total_counts = defaultdict(int)
    for id, cnt in counts.iteritems():
        while id in id_to_pid:
            total_counts[id] += cnt
            id = id_to_pid[id]
        total_counts[id] += cnt


def get_root(id):
    while id in id_to_pid:
        id = id_to_pid[id]
    return id


def path_for_id(id):
    def dir_for_id(genre_id):
        ja_name, en_name = id_to_name.get(id, ('unk', 'unk'))
        dir_name = '%s|%s|%s|%s' % (
            en_name.replace(' ','_'),
            ja_name.replace(' ', '_'),
            id,
            total_counts[id])
        return dir_name

    dirs = []
    while id in id_to_pid:
        dirs.append(dir_for_id(id))
        id = id_to_pid[id]
    dirs.append(dir_for_id(id))

    return '/'.join(dirs[::-1])


print 'WRITING COUNTS...'
with open('total_counts', 'w') as f:
    f.write(str(total_counts))
with open('counts', 'w') as f:
    f.write(str(counts))

print 'WRITING VIZ...'
for l in tqdm(open(descriptions_file), total=20000000):
    l = l.strip()
    id = l.split('\t')[-1]
    path = out_root + '/' + path_for_id(id)
    
    if not os.path.exists(path):
        os.makedirs(path)

    with open(path + '/descriptions.txt', 'a') as f:
        f.write(l + '\n')


