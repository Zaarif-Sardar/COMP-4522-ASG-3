import pymongo
import csv
import pprint


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

    #put data dict info into mongodb collection
    collection = mydb["listings"]
    if "listings" in mydb.list_collection_names():
        print("Collection already exists")

    collection.insert_many(data)


    #Quries 
    #Query 1 
    query1 = collection.find().limit(3) 
    for document in query1:
        print(document)
    #Query 2 
    query2 = collection.find().limit(10) 
    for document in query2:
        pprint.pp(document)
    