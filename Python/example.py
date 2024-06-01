import pandas as pd
from pandas_datareader import data

companies = ["MSFT", "GOOG", "AAPL", "YHOO", "AMZN"]

p = data.DataReader(name = companies, data_source = "google", start = "2010-01-01", end = "2016-12-31")
##thats brings 5 dataframes , con 1709 filas y 5 columnas
##major axis means the range
##minor axis are the companies
p.items
p.major_axis
p.minor_axis
p.axes
p.ndim##to know the number of dimensions
p.dtypes## 
p.shape##to know the len of each dim
p.size## to know the total of values 42200 5*1688*5
p.values

dimensions : 5(items) x 1688(major_axis) x 5(minor_axis)
##Extracting using .loc(labels), .iloc(index), .ix(labels and index)
p.items
p.loc["Close", "2014-04-08", "GOOG"]

p.iloc[3, 200, 3 ] ##first argument '3' brings the 4 item "Close", the second arg "200" brings the position, the third arg is the other axis the stock name in this case MSFT

p.ix["High", 500, "APPL"]

##convert panel to a multiIndex DF
df = p.to_frame()
df.head()

p2 = df.to_panel()

##the major_xs() Method
p.items
p["Volume"]

p.major_xs("2016-09-06")


##the minor_xs() Method
companies = ["MSFT", "GOOG", "AAPL", "YHOO", "AMZN"]

p = data.DataReader(name = companies, data_source = "google", start = "2010-01-01", end = "2016-12-31")

p.minor_axis##brings all the values from the minor axis

p.minor_xs("MSFT")

p.minor_xs("GOOG").head(3)

p["Open"]
p.major_xs("2016-09-16")
p.minor_xs("YHOO")

##Transpose a panel with .transpose() 
p.axes
p2 = p.transpose(2, 1, 0)# this method will change the order 
p
p2
p2["AAPL"]
p2.major_xs("2010-01-04")
p2.minor_xs("Volume")

##The .swapaxes() method
companies = ["MSFT", "GOOG", "AAPL", "YHOO", "AMZN"]

p = data.DataReader(name = companies, data_source = "google", start = "2010-01-01", end = "2016-12-31")\

p2 = p.swapaxes("items", "minor") ## change the values no the structure
p2.axes
p2["MSFT"]
p2.major_xs("2016-09-02")
p2.minor_xs("Close")

###Input and Output
##Feed pd.read_csv() Method a URL Argument

URL = "https://data.cityofnewyork.us/api/views/25th-nujf/rows.csv"
baby_names = pd.read_csv(URL)
baby_names.head()
baby_names["NM"].tolist()
baby_names["NM"].to_frame()
", ".join(str(name) for name in baby_names["NM"])

##Export CSV File with the .to_csv() Method
URL = "https://data.cityofnewyork.us/api/views/25th-nujf/rows.csv"
baby_names = pd.read_csv(URL)
baby_names.head()

baby_names.to_csv("NYC Baby Names.csv", index = False, columns = ["BRTH_YR", "NM", "CNT"], encoding ="utf-8")

##Import Excel File into Pandas
df = pd.read_excel("Data.xlsx")
data = pd.read_excel("Data - multiple worksheets.xlsx", sheetname = ["Data 1", "Data 2"])#this statement is used to bring information from an excel with multiple tabs
data[0]##you can see the information of each worksheet 

##Export Excel File 
URL = "https://data.cityofnewyork.us/api/views/25th-nujf/rows.csv"
baby_names = pd.read_csv(URL)
baby_names.head(3)

girls = baby_names[baby_names["GNDR"] == "FEMALE"]
boys = baby_names[baby_names["GNDR"] == "MALE"]

excel_file = pd.ExcelWriter("Baby Names.xlsx")

girls.to_excel(excel_file, sheet_name = "Girls", index = False)
boys.to_excel(excel_file, sheet_name = "Boys", index = False, columns = ["GNDR", "NM", "CNT"])

excel_file.save()


##Visualization Module
import pandas as pd
from pandas_datareader import data

import matplotlib.pyplot as plt
%matplotlib inline

#The .plot() Method
bb = data.DataReader(name = "BBRY". data_source = "google", start = "2007-07-01", end = "2008-12-05")
bb.head(3)

bb.plot()
bb.plot(y = "Volume")

bb["Volume"].plot()
bb[["High", "Low"]].plot()

plt.style.available

plt.style.use("fivethirtyeight")
bb.plot(y = "Close")

##Bar Charts

google = data.DataReader(name = "GOOG", data_source = "google", start = "2004-01-01", end ="2016-12-31")
google.head(3)

def rank_performance(stock_price)
    if stock_price <= 200:
       return "Poor"
    elif stock_proce > 200 and stock_proce <= 500:
       return "Satisfactory"
    else:
       return "Stellar"

google["Close"].apply(rank_performance).value_counts().plot(kind = "bar") or barh

##Pie Charts

apple = data.DataReader(name = "AAPL", data_source = "google", start = "2012-01-01", end = "2013-03-04")
apple.head(3)

apple["Close"].mean()##this mean method gives us the average

def rank_performance(stock_price):
    if stock_price >= 92.64:
       return "Above Average"
    else:
       return "Below Average"

plt.style.use("ggplot")
apple["Close"].apply(rank_performance).value_counts().plot(kind = "pie", legend = True)

##Histograms
google = data.DataReader(name = "GOOG", data_source = "google", start = "2004-01-01", end ="2016-12-31")
google.head(3)

def custom_round(stock_price):
    return int(stock_price / 100.0) * 100

google["High"].apply(custom_round).value_counts().sort_index()

google["High"].apply(custom_round).nunique()

google["High"].apply(custom_round).plot(kind = "hist", bins = 9)








###Mongo DB with Python

https://realpython.com/introduction-to-mongodb-and-python/
https://www.mongodb.com/blog/post/getting-started-with-python-and-mongodb

import pymongo
myclient = pymongo.MongoClient("mongodb://localhost:27017/") 
mydb = myclient["mydatabase"]

#check if the DB exists
print(myclient.list_database_names())

dblist = myclient.list_database_names()
if "mydatabase" in dblist:
   print("The database exists.")

#A collection in MongoDB is the same as a table in SQL databases.
#creating a collection
import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
 mydb = myclient["mydatabase"]

mycol = mydb["customers"]

#check if the collection exists
collist = mydb.list_collection_names()
if "customers" in collist:
   print("The collection exists.")

#Insert into collection
#A document in MongoDB is the same as a record in SQL databases.
#To insert a record, or document as it is called in MongoDB, into a collection, we use the insert_one() method
import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
 mydb = myclient["mydatabase"]
mycol = mydb["customers"]
mydict = { "name": "John", "address": "Highway 37" }

 x =  mycol.insert_one(mydict)

mydict = { "name": "Peter", "address": "Lowstreet 27" }

 x = mycol.insert_one(mydict)

print(x.inserted_id) 

#insert many documents
#To insert multiple documents into a collection in MongoDB, we use the insert_many() method.
import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
 mydb = myclient["mydatabase"]
mycol = mydb["customers"]
mylist = [
  { "name": "Amy", "address": "Apple st 652"},
   { "name": "Hannah", "address": "Mountain 21"},
  { "name":  "Michael", "address": "Valley 345"},
  { "name": "Sandy", "address":  "Ocean blvd 2"},
  { "name": "Betty", "address": "Green Grass 1"},
   { "name": "Richard", "address": "Sky st 331"},
  { "name": "Susan",  "address": "One way 98"},
  { "name": "Vicky", "address": "Yellow Garden 2"},
  { "name": "Ben", "address": "Park Lane 38"},
   { "name": "William", "address": "Central st 954"},
  { "name":  "Chuck", "address": "Main Road 989"},
  { "name": "Viola",  "address": "Sideway 1633"}
]

x = mycol.insert_many(mylist)

#print list of the _id values of the inserted documents:
print(x.inserted_ids) 

#insert multiple documnts with ID
#If you do not want MongoDB to assign unique ids for you document, you can specify the _id field when you insert the document(s).

import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
 mydb = myclient["mydatabase"]
mycol = mydb["customers"]

mylist = [
  { "_id": 1, "name": "John", "address": "Highway 37"},
   { "_id": 2, "name": "Peter", "address": "Lowstreet 27"},
  { "_id":  3, "name": "Amy", "address": "Apple st 652"},
  { "_id": 4, "name":  "Hannah", "address": "Mountain 21"},
  { "_id": 5, "name":  "Michael", "address": "Valley 345"},
  { "_id": 6, "name": "Sandy",  "address": "Ocean blvd 2"},
  { "_id": 7, "name": "Betty",  "address": "Green Grass 1"},
  { "_id": 8, "name": "Richard",  "address": "Sky st 331"},
  { "_id": 9, "name": "Susan", "address":  "One way 98"},
  { "_id": 10, "name": "Vicky", "address": "Yellow Garden 2"},
  { "_id": 11, "name": "Ben", "address": "Park Lane 38"},
  { "_id": 12, "name": "William", "address": "Central st 954"},
  { "_id": 13, "name": "Chuck", "address": "Main Road 989"},
   { "_id": 14, "name": "Viola", "address": "Sideway 1633"}
]

x = mycol.insert_many(mylist)

#print list of the _id values of the inserted documents:
print(x.inserted_ids) 

##Find option is like the select statement 
#The find_one() method returns the first occurrence in the selection
import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
 mydb = myclient["mydatabase"]
mycol = mydb["customers"]
x = mycol.find_one()

print(x) 

#Find All like select *
import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
 mydb = myclient["mydatabase"]
mycol = mydb["customers"]
for x in mycol.find():
  print(x) 

#Return only some fields
#The second parameter of the find() method is an object describing which fields to include in the result.
import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
 mydb = myclient["mydatabase"]
mycol = mydb["customers"]
for x in mycol.find({},{ "_id": 0, "name": 1, "address": 1 }):
---This example will exclude "address" from the result:
import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
 mydb = myclient["mydatabase"]
mycol = mydb["customers"]
for x in mycol.find({},{ "address": 0 }):
   print(x) 

#Filter the Result
#When finding documents in a collection, you can filter the result by using a query object
import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
 mydb = myclient["mydatabase"]
mycol = mydb["customers"]
myquery = { "address": "Park Lane 38" }
mydoc = mycol.find(myquery)

for x in mydoc:
  print(x) 

   print(x) 

#Advance Query
#To make advanced queries you can use modifiers as values in the query object.
#E.g. to find the documents where the "address" field starts with the letter "S" or higher (alphabetically), use the greater than modifier: {"$gt": "S"}:

import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
 mydb = myclient["mydatabase"]
mycol = mydb["customers"]
myquery = { "address": { "$gt": "S" } }

mydoc = mycol.find(myquery)
for x in mydoc:
  print(x) 


#filter with regular expressions
#Find documents where the address starts with the letter "S":

import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
 mydb = myclient["mydatabase"]
mycol = mydb["customers"]
myquery = { "address": { "$regex": "^S" } }

mydoc = mycol.find(myquery)
for x in mydoc:
  print(x) 

#SORT
#The sort() method takes one parameter for "fieldname" and one parameter for "direction" (ascending is the default direction).

import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
 mydb = myclient["mydatabase"]
mycol = mydb["customers"]
mydoc = mycol.find().sort("name")

for x in mydoc:
  print(x) 


#SORT Descending
#Sort the result reverse alphabetically by name

import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
 mydb = myclient["mydatabase"]
mycol = mydb["customers"]
mydoc = mycol.find().sort("name", -1)

#DELETE
#The first parameter of the delete_one() method is a query object defining which document to delete

import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
 mydb = myclient["mydatabase"]
mycol = mydb["customers"]
myquery = { "address": "Mountain 21" }
mycol.delete_one(myquery) 


for x in mydoc:
  print(x) 


#DELETE Many
#The first parameter of the delete_many() method is a query object defining which documents to delete

import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
 mydb = myclient["mydatabase"]
mycol = mydb["customers"]
myquery = { "address": {"$regex": "^S"} }
x = mycol.delete_many(myquery)

print(x.deleted_count, " documents deleted.") 

#DELETE all the documents in a collection

import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
 mydb = myclient["mydatabase"]
mycol = mydb["customers"]
x = mycol.delete_many({})

print(x.deleted_count, " documents deleted.") 

#DROP
#You can delete a table, or collection as it is called in MongoDB, by using the drop() method

import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
 mydb = myclient["mydatabase"]
mycol = mydb["customers"]
mycol.drop() 

#UPDATE
#The first parameter of the update_one() method is a query object defining which document to update

import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
 mydb = myclient["mydatabase"]
mycol = mydb["customers"]
myquery = { "address": "Valley 345" }
newvalues = { "$set": {  "address": "Canyon 123" } }
mycol.update_one(myquery, newvalues)

#print "customers" after the update:
for x in mycol.find():
  print(x) 

#Update Many
#To update all documents that meets the criteria of the query, use the update_many() method.

import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
 mydb = myclient["mydatabase"]
mycol = mydb["customers"]
myquery = { "address": { "$regex": "^S" } }
newvalues = { "$set": {  "name": "Minnie" } }
x = mycol.update_many(myquery, newvalues)

print(x.modified_count, "documents updated.") 

#Limit the Result
#The limit() method takes one parameter, a number defining how many documents to return

import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["mydatabase"]
mycol = mydb["customers"]
myresult = mycol.find().limit(5)

#print the result:
for x in myresult:
  print(x) 

##List

mylist = [1,2,['x','y','z']]
print(mylist[2][1])
matrix = [[1,2,3],[4,5,6],[7,8,9]]
matrix[0][0]
first_col = [row[0] for row in matrix]
print(first_col)

##Dictionaries
my_stuff = {"key1":"value","key2" :"value2"}
print(my_stuff["key2"])

my_stuff = {"key1":123,"key2":"value2","key3":{"123":[1,2,3]}}

#####Visualizing Stock Data

from matplotlib.dates import DateFormatter, WeekdayLocator,\
    DayLocator, MONDAY
from matplotlib.finance import candlestick_ohlc
def pandas_candlestick_ohlc(dat, stick = "day", otherseries = None):
    """
    :param dat: pandas DataFrame object with datetime64 index, and float columns "Open", "High", "Low", and "Close", likely created via DataReader from "yahoo"
    :param stick: A string or number indicating the period of time covered by a single candlestick. Valid string inputs include "day", "week", "month", and "year", ("day" default), and any numeric input indicates the number of trading days included in a period
    :param otherseries: An iterable that will be coerced into a list, containing the columns of dat that hold other series to be plotted as lines
    This will show a Japanese candlestick plot for stock data stored in dat, also plotting other series if passed.
    """
    mondays = WeekdayLocator(MONDAY)        # major ticks on the mondays
    alldays = DayLocator()              # minor ticks on the days
    dayFormatter = DateFormatter('%d')      # e.g., 12
    # Create a new DataFrame which includes OHLC data for each period specified by stick input
    transdat = dat.loc[:,["Open", "High", "Low", "Close"]]
    if (type(stick) == str):
        if stick == "day":
            plotdat = transdat
            stick = 1 # Used for plotting
        elif stick in ["week", "month", "year"]:
            if stick == "week":
                transdat["week"] = pd.to_datetime(transdat.index).map(lambda x: x.isocalendar()[1]) # Identify weeks
            elif stick == "month":
                transdat["month"] = pd.to_datetime(transdat.index).map(lambda x: x.month) # Identify months
            transdat["year"] = pd.to_datetime(transdat.index).map(lambda x: x.isocalendar()[0]) # Identify years
            grouped = transdat.groupby(list(set(["year",stick]))) # Group by year and other appropriate variable
            plotdat = pd.DataFrame({"Open": [], "High": [], "Low": [], "Close": []}) # Create empty data frame containing what will be plotted
            for name, group in grouped:
                plotdat = plotdat.append(pd.DataFrame({"Open": group.iloc[0,0],
                                            "High": max(group.High),
                                            "Low": min(group.Low),
                                            "Close": group.iloc[-1,3]},
                                           index = [group.index[0]]))
            if stick == "week": stick = 5
            elif stick == "month": stick = 30
            elif stick == "year": stick = 365
    elif (type(stick) == int and stick >= 1):
        transdat["stick"] = [np.floor(i / stick) for i in range(len(transdat.index))]
        grouped = transdat.groupby("stick")
        plotdat = pd.DataFrame({"Open": [], "High": [], "Low": [], "Close": []}) # Create empty data frame containing what will be plotted
        for name, group in grouped:
            plotdat = plotdat.append(pd.DataFrame({"Open": group.iloc[0,0],
                                        "High": max(group.High),
                                        "Low": min(group.Low),
                                        "Close": group.iloc[-1,3]},
                                       index = [group.index[0]]))
    else:
        raise ValueError('Valid inputs to argument "stick" include the strings "day", "week", "month", "year", or a positive integer')
    # Set plot parameters, including the axis object ax used for plotting
    fig, ax = plt.subplots()
    fig.subplots_adjust(bottom=0.2)
    if plotdat.index[-1] - plotdat.index[0] < pd.Timedelta('730 days'):
        weekFormatter = DateFormatter('%b %d')  # e.g., Jan 12
        ax.xaxis.set_major_locator(mondays)
        ax.xaxis.set_minor_locator(alldays)
    else:
        weekFormatter = DateFormatter('%b %d, %Y')
    ax.xaxis.set_major_formatter(weekFormatter)
    ax.grid(True)
    # Create the candelstick chart
    candlestick_ohlc(ax, list(zip(list(date2num(plotdat.index.tolist())), plotdat["Open"].tolist(), plotdat["High"].tolist(),
                      plotdat["Low"].tolist(), plotdat["Close"].tolist())),
                      colorup = "black", colordown = "red", width = stick * .4)
    # Plot other series (such as moving averages) as lines
    if otherseries != None:
        if type(otherseries) != list:
            otherseries = [otherseries]
        dat.loc[:,otherseries].plot(ax = ax, lw = 1.3, grid = True)
    ax.xaxis_date()
    ax.autoscale_view()
    plt.setp(plt.gca().get_xticklabels(), rotation=45, horizontalalignment='right')
    plt.show()
pandas_candlestick_ohlc(apple)


# Definimos unos cuantos clientes
clientes= [
 {'Nombre': 'Hector', 'Apellidos':'Costa Guzman', 'dni':'11111111A'},
 {'Nombre': 'Juan', 'Apellidos':'González Márquez', 'dni':'22222222B'} 
]

# Creamos una función que muestra un cliente en una lista a partir del DNI
def mostrar_cliente(clientes, dni):
 for c in clientes:
  if (dni == c['dni']):
   print('{} {}'.format(c['Nombre'],c['Apellidos']))
 return
 print('Cliente no encontrado')

# Creamos una función que borra un cliente en una lista a partir del DNI
def borrar_cliente(clientes, dni):
 for i,c in enumerate(clientes):
  if (dni == c['dni']):
   del( clientes[i] )
 print(str(c),"> BORRADO")
 return

 print('Cliente no encontrado') 


 class Cliente:

 def __init__(self, dni, nombre, apellidos):
 self.dni = dni
 self.nombre = nombre
 self.apellidos = apellidos

 def __str__(self):
 return '{} {}'.format(self.nombre,self.apellidos)

# Y otra para las empresas
class Empresa:

    def __init__(self, clientes=[]):
     self.clientes = clientes

    def mostrar_cliente(self, dni=None):
     for c in self.clientes:
      if c.dni == dni:
       print(c)
       return
    print("Cliente no encontrado")

    def borrar_cliente(self, dni=None):
     for i,c in enumerate(self.clientes):
      if c.dni == dni:
       del(self.clientes[i])
       print(str(c),"> BORRADO")
     return
    print("Cliente no encontrado")

    ### Ahora utilizaré ambas estructuras 

    # Creo un par de clientes
    hector = Cliente(nombre="Hector", apellidos="Costa Guzman", dni="11111111A")
    juan = Cliente("22222222B", "Juan", "Gonzalez Marquez")

    # Creo una empresa con los clientes iniciales
    empresa = Empresa(clientes=[hector, juan])

    # Muestro todos los clientes
    print("==LISTADO DE CLIENTES==")
    print(empresa.clientes)

    print("\n==MOSTRAR CLIENTES POR DNI==")
    # Consulto clientes por DNI
    empresa.mostrar_cliente("11111111A")
    empresa.mostrar_cliente("11111111Z")

    print("\n==BORRAR CLIENTES POR DNI==")
    # Borro un cliente por DNI
    empresa.borrar_cliente("22222222V")
    empresa.borrar_cliente("22222222B")

    # Muestro de nuevo todos los clientes
    print("\n==LISTADO DE CLIENTES==")
    print(empresa.clientes)

# For example, given A = [1, 3, 6, 4, 1, 2], the function should return 5.

# Given A = [1, 2, 3], the function should return 4.

# Given A = [−1, −3], the function should return 1.

# Write an efficient algorithm for the following assumptions:

# N is an integer within the range [1..100,000];
# each element of array A is an integer within the range [−1,000,000..1,000,000].


def solution(A):
    # Iterate through the array and mark the presence of positive integers
    for i in range(len(A)):
        while 0 < A[i] <= len(A) and A[A[i] - 1] != A[i]:
            A[A[i] - 1], A[i] = A[i], A[A[i] - 1]
    
    # Find the smallest positive integer not present in the array
    for i in range(len(A)):
        if A[i] != i + 1:
            return i + 1
    
    # If all positive integers from 1 to N are present, return N + 1
    return len(A) + 1