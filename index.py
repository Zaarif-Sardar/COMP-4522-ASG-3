import pymongo
import csv


database = pymongo.MongoClient("mongodb://localhost:27017/")

dbList = database.list_database_names()

if "TheDatabase" in dbList:
    print("Database already exists")
else:
    print("Creating new Database")
    mydb = database["TheDatabase"]
    print(database.list_database_names())

    #put csv info into a data dict 
    with open("listings.csv",'r') as file:
        csv_read = csv.DictReader(file)
        data = [row for row in csv_read]

    #put data dict info into mongodb
    collection = mydb["listings"]
    if "listings" in mydb.list_collection_names():
        print("Collection already exists")

    collection.insert_many(data)



 