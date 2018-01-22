"""
my janky version of map reduce
"""
import glob
from tqdm import tqdm
import multiprocessing
import os
from joblib import Parallel, delayed

def mapreduce(
    map_fn, 
    reduce_fn, 
    input_filename='', 
    output_filename='',   # write all outputs to this file
    input_re='', 
    mirror_outputs=False,  # if true, write outputs for each input file
    split_lines=50000
):
    assert len(input_filename + input_re) > 0, 'must provide input file'
    assert len(output_filename + output_re) > 0, 'must provide output file'
    
    n_cores = multiprocessing.cpu_count()    
    paths = glob.glob(input_re) if input_re else [input_filename]

    results = []
    for i, path in tqdm(enumerate(paths)):
        print 'INFO: starting ', path

        print 'SPLITTING...'
        os.system('split -l %s part_%s_' % (path, i))

        dir = os.path.dirname(path)
        parts_re = os.path.join(dir, 'parts_*')
        part_paths = glob.glob(parts_re)

        # result = list of strings
        result = Parallel(n_jobs=n_cores)(
            delayed(map_fn)(part for part in tqdm(part_paths)))

        if mirror_outputs:
            with open(os.path.join(path, '.out'), 'w') as f:
                f.write('\n'.join(result) + '\n')
        else:
            results += result

    print 'DUMPING OUTPUT'
    with open(output_filename, 'w') as f:
        f.write('\n'.join(results) + '\n')
                
