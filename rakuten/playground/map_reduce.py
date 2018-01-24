"""
my janky version of map reduce
"""
import glob
from tqdm import tqdm
import multiprocessing
import os
from joblib import Parallel, delayed

def mapreduce(
    map_fn,    # takes a path, returns something
    reduce_fn, # takes a list of mapper outputs, returns a reduced string
    input_filename='',    # input filename (will be split)
    input_re='',          # input re (grabs all matches)
    output_filename='',   # write all outputs to this file
    split_lines=50000,
    n_cores=16
):
    assert len(input_filename + input_re) > 0, 'must provide input file'
    
    if input_re:
        input_paths = glob.glob(input_re)
    else:
        cmd = 'split -l %s %s part_%s_' % (split_lines, path, i)
        print 'SPLITTING with ', cmd
        os.system(cmd)
        input_paths = glob.glob('part_*')

    results = Parallel(n_jobs=n_cores)(
        delayed(map_fn)(part) for part in tqdm(input_paths))

    result_str = reduce_fn(results)

    # cleanup if we made splits
    if len(input_re) == 0:
        os.system('rm ' + ' '.join(input_paths))

    if output_filename:
        print 'WRITING TO ', output_filename
        with open(output_filename, 'w') as f:
            f.write(result_str)
                
