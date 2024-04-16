#-------------------------------------------------------------------------
# AUTHOR: Joel Joshy
# FILENAME: db_conntection_mongo.py
# SPECIFICATION: Assignment #3
# FOR: CS 4250- Assignment #3
# TIME SPENT: 2 hours
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
# --> add your Python code here
from pymongo import MongoClient
import re;

def connectDataBase():

    # Create a database connection object using pymongo
    # --> add your Python code here
    client = MongoClient(host="localhost", port=27017)
    db = client.Assignment3_CS4250
    return db

def createDocument(col, docId, docText, docTitle, docDate, docCat):

    dictionaryOfTermsAndCounts = {}

    # create a dictionary indexed by term to count how many times each term appears in the document.
    # Use space " " as the delimiter character for terms and remember to lowercase them.
    # --> add your Python code here
    for term in docText.split(' '):
        cleanedTerm = re.sub(r'[^\w\s]', '', term.lower())
        if cleanedTerm not in dictionaryOfTermsAndCounts:
            dictionaryOfTermsAndCounts[cleanedTerm] = 1
        else:
            dictionaryOfTermsAndCounts[cleanedTerm] += 1

    # create a list of objects to include full term objects. [{"term", count, num_char}]
    # --> add your Python code here
    listOfTermObjs = []
    for term in dictionaryOfTermsAndCounts.keys():
        listOfTermObjs.append({"term": term, "count": dictionaryOfTermsAndCounts[term], "num_char": len(term)})
        
    # produce a final document as a dictionary including all the required document fields
    # --> add your Python code here
    finalDocumentObj = {"_id": docId, "text": docText, "title": docTitle, "date": docDate, "category": docCat, "terms": listOfTermObjs}

    # insert the document
    # --> add your Python code here
    result = col.insert_one(finalDocumentObj)
    print(result.inserted_id)

def deleteDocument(col, docId):

    # Delete the document from the database
    # --> add your Python code here
    col.delete_one({"_id": docId})



def updateDocument(col, docId, docText, docTitle, docDate, docCat):

    # Delete the document
    # --> add your Python code here
    deleteDocument(col, docId)

    # Create the document with the same id
    # --> add your Python code here
    createDocument(col, docId, docText, docTitle, docDate, docCat)

def getIndex(col):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    # --> add your Python code here
   
    pipeline = [
        {"$unwind": {"path":"$terms"}},
        {"$project": {"_id": 0, "text": 0, "num_chars": 0, "category": 0, "date": 0}},
        {"$sort": {"terms.term": 1}}
    ]
    
    results = col.aggregate(pipeline)
    termAndDocumentCount = {}
    
    for result in results:
        term = result['terms']['term']
        title = result['title']
        termCount = result['terms']['count']
        if term in termAndDocumentCount:
            termAndDocumentCount[term] += f", {title} : {termCount}"
        else:
            termAndDocumentCount[term] = f"{title}: {termCount}"
    return termAndDocumentCount
            




