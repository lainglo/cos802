import csv

with open('/cos802/EX/EXP_TWEETS_DETAIL/data.csv') as csv_file:
  csv_reader = csv.reader(csv_file, delimiter=',')
  line_count = 0
  for row in csv_reader:
      print(",".join(row))
      line_count += 1
      if line_count == 500000:
       break

