import cassandra
from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect()

KEYSPACE = 'streaming'

session.execute("drop keyspace if exists streaming")

session.execute("""
    CREATE KEYSPACE %s
    WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
    """ % KEYSPACE)

session.set_keyspace(KEYSPACE)
session.execute("create table movie(title text, director text, year text, EIDR text PRIMARY KEY)")

def printSomething():
    print("Print Something")
    session.execute("insert into movie(title, director, year, EIDR) VALUES (%s, %s, %s, %s)", ('Schindlers list', 'Steven Spielberg', '1993', '1'))
    session.execute("insert into movie(title, director, year, EIDR) VALUES (%s, %s, %s, %s)", ('Inception', 'Chrisopher Nolan', '2010', '2'))

def addMovie(title, director, year, EIDR):
    print("Add Movie")
    session.execute("insert into movie(title, director, year, EIDR) VALUES (%s, %s, %s, %s)", (title, director, year, EIDR))

def removeMovie(EIDR):
    print("Remove Movie")
    session.execute("DELETE FROM movie where EIDR = %s", EIDR)

def editMovie(title, director, year, EIDR):
    print("Edit Movie")
    

def searchMovieByTitle(title):
    print("Search by title")

def searchMovieByDirector(director):
    print("Search by director")

def searchMovieByYear(year):
    print("Search by year")

def searchMovieByEIDR(EIDR):
    print("Search by EIDR")

def sortByTitle(title):
    print("Sort by title")

def sortByDirector(director):
    print("Sort by title")

def sortByYear(year):
    print("Sort by title")

def sortByEIDR(EIDR):
    print("Sort by title")


while True:
    inp = raw_input('command?').lstrip().split(',')
    i = 0;
    for i in range(len(inp)):
        inp[i] = inp[i].strip()
    command = inp[0]
    if command == 'print something':
        printSomething()
    elif command == 'add movie':
        if len(inp) == 5:
            addMovie(inp[1], inp[2], inp[3], inp[4])
        else:
            print('Incorrect number of arguments')
    elif command == 'remove movie':
        if len(inp) == 2:
            removeMovie(inp[1])
        else:
            print('Incorrect number of arguments')
    elif command == 'quit':
        break
    else:
        print('Invalid Command.')