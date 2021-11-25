#-*- coding:utf-8 -*-
from starbase import Connection
import csv

c = Connection(host='127.0.0.1', port=8001)
ratings = c.table('ratings')
if (ratings.exists()):
    ratings.drop()
ratings.create('rating')

batch = ratings.batch()
print(ratings.batch())
if batch:
    print("Batch update... \n")
    with open("../data/ratings.csv", "r") as f:
        reader = csv.reader(f, delimiter=',')
        next(reader)
        for row in reader:
            batch.update(row[0], {'rating': {row[1]: row[2]}})
    
    print("Committing...\n")
    batch.commit(finalize=True)

    print("Get ratings for users...\n")
    print("Ratings for UserID 1: ")
    print(ratings.fetch("1"))

    print("\n")
    print("Ratings for UserID 33: ")
    print(ratings.fetch("33"))