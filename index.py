import pymongo
import csv
import pprint


database = pymongo.MongoClient("mongodb://localhost:27017/")

dbList = database.list_database_names()

if "TheDatabase" not in dbList:
    print("Creating new Database")
    mydb = database["TheDatabase"]
    print(database.list_database_names())

    #put csv info into a data dict 
    with open("listings.csv",'r', encoding='utf-8') as file:
        csv_read = csv.DictReader(file)
        data = [row for row in csv_read]

    #put data dict info into mongodb collection
    collection = mydb["listings"]
    if "listings" in mydb.list_collection_names():
        print("Collection already exists")
        collection.insert_many(data)

    #converting beds from string to int for query 5 processing
    collection.update_many(
        {},
        [{"$set": { "beds": { "$toInt": "$beds" } }}]
    )
    
    
else:
    print("Database already exists")
    mydb = database["TheDatabase"]
    collection = mydb["listings"]
    

    #Quries 
    #Query 1 
    query1 = collection.find().limit(3) 
    for document in query1:
        print(document)
    #Query 2 
    query2 = collection.find().limit(10) 
    for document in query2:
        pprint.pp(document)
    #Query 3 
    query3 = collection.find({'host_is_superhost': 't'}, {'host_id': 1}).limit(2)
    superhost_ids = [host['host_id'] for host in query3]

    for host_id in superhost_ids:
        lists = collection.find({'host_id': host_id})
        for listing in lists:
            pprint.pp(listing)
    
    #query 4
    query4 = collection.distinct("host_name")
    print("Unique host names: ")
    for host_name in query4: print(host_name)


    #query 5
    neighbourhood = "Chamartín" #Chamartín is an example of one cleansed_group. you can use others here.
    query5 = collection.find(
        {
            "beds": {"$gt": 2},
            "neighbourhood_group_cleansed": neighbourhood
        }
    ).sort("review_scores_rating", -1)

    print(f"Places with more than 2 beds in {neighbourhood}, sorted by review_scores_rating (desc):")
    for place in query5: pprint.pp(place)
    


