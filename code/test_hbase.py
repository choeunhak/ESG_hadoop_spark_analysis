#-*- coding:utf-8 -*-
from starbase import Connection
import csv

print("연결시작")
c = Connection(host='localhost', port=8001)
print(c)
print("연결성공")
ratings = c.table('ratings')
# if (ratings.exists()):
#     ratings.drop()
ratings.create('rating')
print('배치시작')
print(ratings)
batch = ratings.batch(fail_silently=False)
print(ratings.batch())
print(batch)
if batch:
    print("Batch update... \n")
    with open("./ratings.csv", "r") as f:
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