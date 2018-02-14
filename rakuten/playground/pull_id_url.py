descriptions = '/Users/rpryzant/Documents/rakuten_data/clothing_products.tok'


for l in open(descriptions):
    parts = l.strip().split('\t')
    print '%s\t%s' % (parts[1], parts[5])
