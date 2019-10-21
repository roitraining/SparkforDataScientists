d = {'job':'unemployed', 'marital':'divorced', 'amount':100, 'sum':200}
#{'job': {'unemployed': 8.0, 'entrepreneur': 9.0, 'services': 4.0, 'retired': 5.0, 'management': 0.0, 'unknown': 11.0, 'self-employed': 6.0, 'student': 7.0, 'blue-collar': 1.0, 'admin.': 3.0, 'housemaid': 10.0, 'technician': 2.0}, 'marital': {'married': 0.0, 'divorced': 2.0, 'single': 1.0}, 'education': {'secondary': 0.0, 'unknown': 3.0, 'tertiary': 1.0, 'primary': 2.0}
import imp
imp.reload(pyh)

def makeSparseVector(index, count):
    from  pyspark.mllib.linalg import SparseVector

    if index == count - 1:
        return SparseVector(count - 1, [], [])
    return SparseVector(count - 1, [index], [1.0])

def predictOne(model, keydict, **kwargs):
    
    from pyspark.ml.feature import VectorAssembler
    x = kwargs.copy()

    categorical_features = []
    for key in keydict:
        if key in kwargs.keys():
            x[key] = keydict[key].get(kwargs[key])
            #print ('*', x, key, x[key])
            categorical_features.append(key)
            print ('***', key, x[key], keydict[key], keydict[key].keys())
            x[key] = makeSparseVector(x[key], len(keydict[key].keys()))
            
    df = spark.createDataFrame(sc.parallelize([x]))
    print(df.take(1))
    numeric_features = list(set(kwargs.keys()).difference(set(categorical_features)))
    print(categorical_features, numeric_features)
    
    assemblerInputs = numeric_features + categorical_features 
    assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
    df = assembler.transform(df).drop(*(numeric_features)) # + categorical_features))
    df.printSchema()
    print('--->', df.take(1))
    
#     for col in categorical_features:
#         df = df.withColumnRenamed(col, col + '_Vector')
#     df = AssembleFeatures(df, categorical_features, numeric_features).select('features')
#     df.show()
    #z = pyh.MakeMLDataFrame(df, categorical_columns, numeric_columns, None, target_is_categorical = False, return_key_dict = False, skip_string_indexer = True)
    #return z
    #print(z.take(1))
    #predictions = model.transform(z)
    #return predictions

    
#help(SparseVector)
# x = SparseVector(1, [1,0])
# print (x)
#from pyspark.mllib.
#print(SparseVector)

predictOne(None, keydict, age = 30, marital = 'single', job = 'admin.', duration = 1042)

# predict = [dict(age=30,marital='single'), dict(age=59,marital='married'), dict(age=70,marital='divorced'), dict(age=90,marital='widow')]
# predict = spark.createDataFrame(sc.parallelize(predict))
# predict.show()
# x, x0 = pyh.StringIndexEncode(predict, ['marital'], return_key_dict = True)
# x.show()
# print (x0)
# y = pyh.OneHotEncode(x, ['marital'])
# y.show()

# # predict = dict(age=59, balance=2343, duration=1042, pdays=-1, job='admin.', marital='married', education='secondary', housing='yes', loan='no', contact='unknown', campaign=1, poutcome='unknown', deposit='yes')
# # print(predict)
# # predict = spark.createDataFrame(sc.parallelize([predict]))
# # print(predict)
# # x = dtModel.transform(predict)

# # x = predictOne(None, keydict, **d)
# # x.printSchema()
# # print(x.columns)
# # print(type(x.features))
# # help(x)
# # x = predictOne(dtModel, keydict, age=59, balance=2343, duration=1042, pdays=-1, job='admin.', marital='married', education='secondary', housing='yes', loan='no', contact='unknown', campaign=1, poutcome='unknown', deposit='yes')
# # print(x)
# # print(x.count())
# # x.show()



# #[Row(label=0.0, features=SparseVector(63, {3: 1.0, 11: 1.0, 13: 1.0, 17: 1.0, 19: 1.0, 20: 1.0, 55: 1.0, 59: 59.0, 60: 2343.0, 61: 1042.0, 62: -1.0}))]
# #[Row(age=59, balance=2343, duration=1042, pdays=-1, job='admin.', marital='married', education='secondary', housing='yes', loan='no', contact='unknown', campaign=1, poutcome='unknown', deposit='yes', default='no')]
# print (y.take(1))

# import numpy as np
# a = np.array([1,0])
# print (a)
# b = y.take(1)[0]['marital_Vector']
# print (b)
# a = SparseVector(2,[0.0],[1.0]) 
# print(a)
# print(type(a), type(b))
# print (a == b)
# #help(a)

# def makeSparseVector(index, count):
#     if index == count - 1:
#         return SparseVector(count - 1, [], [])
#     return SparseVector(count - 1, [index], [1.0])

# print (makeSparseVector(0, 4))
# print (makeSparseVector(1, 4))
# print (makeSparseVector(2, 4))
#print (makeSparseVector(3, 4))

