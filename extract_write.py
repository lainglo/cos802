import csv

infile = '/cos802/EX/EXP_TWEETS_DETAIL/data.csv'
outfile = 'tweet_subset.csv'

with open(infile, encoding='utf-8') as f, open(outfile, 'w') as o:
    count = 0
    reader = csv.reader(f)
    writer = csv.writer(o, delimiter=',') # adjust as necessary
    for row in reader:
      writer.writerow(row)
      count += 1
      if count > 500:
       break
# no need for close statements
print('Done')
