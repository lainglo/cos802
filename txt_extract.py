fh = open('/cos802/EX/EXP_TWEETS_DETAIL/data.csv')
count = 0
for line in fh:
    print(line)
    count += 1
    if count == 5000000:
     break
fh.close()


