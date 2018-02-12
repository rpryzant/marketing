"""
cleans/processes descriptions
"""
from tqdm import tqdm
import re
import Mykytea

descriptions_file = '/Users/rpryzant/Documents/rakuten_data/clothing_products'
out_file = '/Users/rpryzant/Documents/rakuten_data/clothing_products.tok'

def kytea_tokenize(s):
    return ' '.join(list(mk.getWS(s)))

out = open(out_file, 'a')

mk = Mykytea.Mykytea('-notags')
for l in tqdm(open(descriptions_file), total=20000000):
    l = l.strip()
    parts = l.split('\t')

    if len(parts) != 11: continue

    description = re.sub('\s+', ' ', parts[3])
    description2 = re.sub('\s+', ' ', parts[4])

    description = description.replace('\N', '')
    description2 = description2.replace('\N', '')

    parts[3] = kytea_tokenize(description)
    parts[4] = kytea_tokenize(description2)

    out.write('\t'.join(parts) + '\n')

out.close()
