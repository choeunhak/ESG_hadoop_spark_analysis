#-*- coding:utf-8 -*-
import happybase
import csv

c = happybase.Connection(host='localhost',port=8001)
c.open()
ratings = c.table('ratings')
c.create_table('rating',
    {
     'ratingss': dict(),
    })
batch = ratings.batch()


if batch:
    print("Batch update... \n")
    with open("./ratings.csv", "r") as f:
        reader = csv.reader(f, delimiter=',')
        next(reader)
        for row in reader:
            batch.put(row[0], {'rating': {row[1]: row[2]}})
    
    print("Committing...\n")
    batch.send()

    print("Get ratings for users...\n")
    print("Ratings for UserID 1: ")
    print(ratings.row("1"))

    print("\n")
    print("Ratings for UserID 33: ")
    print(ratings.row("33"))

