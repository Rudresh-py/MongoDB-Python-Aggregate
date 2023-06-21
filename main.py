from pymongo import MongoClient

mongo_uri = "mongodb://localhost:27017/analytics"

client = MongoClient(mongo_uri)
db = client.get_default_database()
persons_collection = db["persons"]

pipeline1 = [
    {'$match': {'gender': 'female'}},
]

pipeline2 = [
    {'$match': {'gender': 'female'}},
    {'$group': {'_id': {'state': '$location.state'}, 'totalpersons': {'$sum': 1}}},
]

pipeline3 = [
    {'$match': {'gender': 'female'}},
    {'$group': {'_id': {'state': '$location.state'}, 'totalpersons': {'$sum': 1}}},
    {'$sort': {'totalpersons': -1}}
]

pipeline4 = [
    {'$project': {'_id': 0, 'gender': 1, 'fullName': {'$concat': ["$name.first", " ", "$name.last"]}}}
]

pipeline5 = [
    {'$project': {'_id': 0, 'gender': 1,
                  'fullName': {'$concat': [{"$toUpper": "$name.first"}, " ", {"$toUpper": "$name.last"}]}}}
]

pipeline6 = [
    {'$project': {'_id': 0, 'gender': 1,
                  'fullName': {'$concat': [{"$toUpper": {'$substrCP': ["$name.first", 0, 1]}},
                                           {
                                               '$substrCP': ['$name.first', 1, {
                                                   '$subtract': [{'$strLenCP': '$name.first'}, 1]
                                               }]
                                           },
                                           " ",
                                           {"$toUpper": {'$substrCP': ["$name.last", 0, 1]}},
                                           {
                                               '$substrCP': ['$name.last', 1, {
                                                   '$subtract': [{'$strLenCP': '$name.last'}, 1]
                                               }]
                                           }]}}}
]

pipeline7 = [
    {'$group': {'_id': {'age': "$age"}, 'allHobbies': {'$push': '$hobbies'}}}
]

pipeline11 = [
    {'$bucket': {'groupBy': "$dob.age",
                 'boundaries': [0, 18, 30, 50, 80, 120],
                 'output': {
                     'numPersons': {'$sum': 1},
                     'averageAge': {'$avg': '$dob.age'},
                 }}}
]


result = persons_collection.aggregate(pipeline11)
c = 0
for p in result:
    print(p)
    c = c + 1
print(c)
"""----------------------------------------------------------------------------------"""
pipeline8 = [
    {"$project": {'_id': 0, 'examScore': {'$slice': ["$examScores", 1]}}}
]

pipeline9 = [
    {"$project": {'_id': 0, 'numScore': {'$size': "$examScores"}}}
]

pipeline10 = [
    {
        '$project': {
            '_id': 0,
            'scores': {
                '$filter': {
                    'input': '$examScores',
                    'as': 'sc',
                    'cond': {'$gt': ['$$sc.score', 60]}
                }
            }
        }
    }
]

result = db.friends.aggregate(pipeline9)
c = 0
for p in result:
    # print(p)
    c = c + 1
# print(c)
