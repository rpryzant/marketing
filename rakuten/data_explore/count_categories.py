import map_reduce
from collections import defaultdict


def map_fn(path):
    out = defaultdict(int)
    for l in open(path):
        parts = l.strip().split("\t")
        genre_id = parts[-1]
        out[genre_id] += 1

    with open(path + '.out', 'w') as f:
        f.write(str(dict(out)))

    return out


def reduce_fn(result_list):
    counts = defaultdict(int)
    for result_dict in result_list:
        for genre_id, count in result_dict.iteritems():
            counts[genre_id] += count
    return str(dict(counts))


map_reduce.mapreduce(
    map_fn=map_fn,
    reduce_fn=reduce_fn,
    input_re='/Users/rpryzant/Desktop/test/*.tsv',
#    input_re='/Volumes/datasets/rakuten_dump/merchandize_data/products_tsv/*.tsv',
    output_filename='/Users/rpryzant/Desktop/test/out'
)
