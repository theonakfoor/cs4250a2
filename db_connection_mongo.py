#-------------------------------------------------------------------------
# AUTHOR: Theo Nakfoor
# FILENAME: db_connection_mongo.py
# SPECIFICATION: A simple program to handle creation, update, deletion and index generation commands for a MongoDB database.
# FOR: CS 4250- Assignment #2
# TIME SPENT: 1h
#-----------------------------------------------------------*/

from pymongo import MongoClient  # import mongo client to connect

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
# --> add your Python code here

def connectDataBase():
    client = MongoClient("localhost", 27017)
    return client["local"]

def createDocument(col, docId, docText, docTitle, docDate, docCat):
    terms = []
    docTextCleaned = "".join([ t if (t.isalnum() or t.isspace()) else "" for t in docText])
    for term in set(docTextCleaned.lower().split(" ")):
        terms.append({
            "term": term,
            "count": docText.lower().count(term),
            "num_chars": len(term)
        })

    document = {
        "_id": docId,
        "title": docTitle,
        "text": docText,
        "num_chars": len(docText),
        "date": { "$date": docDate },
        "category": docCat,
        "terms": terms
    }

    col.insert_one(document)

def deleteDocument(col, docId):
    col.delete_one({ "_id": docId })

def updateDocument(col, docId, docText, docTitle, docDate, docCat):
    deleteDocument(col, docId)
    createDocument(col, docId, docText, docTitle, docDate, docCat)

def getIndex(col):
    cursor = col.aggregate([{ "$unwind": "$terms" }, { "$group": { "_id": "$terms.term", "documents": { "$push": { "$concat": ["$title", ":", { "$toString": "$terms.count"}] } } } }, { "$project": { "_id": 0, "term": "$_id", "documents": 1}}])
    return { record["term"]:record["documents"] for record in list(cursor)}