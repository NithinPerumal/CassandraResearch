import cassandra
from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect()

KEYSPACE = 'streaming'

session.execute("drop keyspace if exists streaming")

session.execute("""
    CREATE KEYSPACE %s
    WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
    """ % KEYSPACE)

session.set_keyspace(KEYSPACE)
session.execute("create table movies(title text, director text, year text, EIDR text PRIMARY KEY)")

def printSomething():
    print("Print Something")
    session.execute("insert into movies(title, director, year, EIDR) VALUES (%s, %s, %s, %s)", ('Schindlers list', 'Steven Spielberg', '1993', '1'))
    session.execute("insert into movies(title, director, year, EIDR) VALUES (%s, %s, %s, %s)", ('Inception', 'Chrisopher Nolan', '2010', '2'))
    session.execute("insert into movies(title, director, year, EIDR) VALUES (%s, %s, %s, %s)", ('Up', 'Pete Doctor', '2009', '3'))

printSomething()

def addMovie(title, director, year, EIDR):
    print("Add Movie")
    session.execute("insert into movies(title, director, year, EIDR) VALUES (%s, %s, %s, %s)", (title, director, year, EIDR))

def removeMovie(EIDR):
    print("Remove Movie")
    session.execute("DELETE FROM movies where EIDR = %s", EIDR)

def editMovie(title, director, year, EIDR):
    print("Edit Movie")
    # ret = session.execute("Select title, director, year, EIDR from movies where EIDR=%s", EIDR)
    # print(ret.current_rows)
    # for item in ret:
    #     print (item.title)
    session.execute("UPDATE movies SET title=%s, director=%s, year=%s where EIDR=%s IF EXISTS", (title, director, year, EIDR))

def searchMovieByTitle(title):
    print("Search by title")
    ret = session.execute("""SELECT * from streaming.movies where title=%s ALLOW FILTERING""", (title,))
    print(ret.current_rows)

def searchMovieByDirector(director):
    print("Search by director")
    ret = session.execute("""SELECT * from streaming.movies where director=%s ALLOW FILTERING""", (director,))
    print(ret.current_rows)

def searchMovieByYear(year):
    print("Search by year")
    ret = session.execute("""SELECT * from streaming.movies where year=%s ALLOW FILTERING""", (year,))
    print(ret.current_rows)

def searchMovieByEIDR(EIDR):
    print("Search by EIDR")
    ret = session.execute("""SELECT * from streaming.movies where EIDR=%s ALLOW FILTERING""", (EIDR,))
    print(ret.current_rows)

def sortByTitle():
    print("Sort by title")
    ret = session.execute("""Select * from streaming.movies""")
    print(ret.current_rows)

    allData = ret.current_rows;

    allTitles = []
    for item in ret:
        allTitles.append(item.title)
    print(allTitles)
    sortedTitles = sorted(allTitles)
    print(sortedTitles)

    if not ret.current_rows:
        print("hihi")

    print(len(ret.current_rows))
    for item in ret.current_rows:
        print("items")
        print(item)

def sortByDirector():
    print("Sort by title")

def sortByYear():
    print("Sort by title")

def sortByEIDR():
    print("Sort by title")
    ret = session.execute("""Select * from streaming.movies ORDER BY EIDR ASC""")
    print(ret.current_rows)


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
    elif command == 'edit movie':
        if len(inp) == 5:
            editMovie(inp[1], inp[2], inp[3], inp[4])
        else:
            print('Incorrect number of arguments')
    elif command == 'search by title':
        if len(inp) == 2:
            print(searchMovieByTitle(inp[1]))
        else:
            print('Incorrect number of arguments')
    elif command == 'search by director':
        if len(inp) == 2:
            print(searchMovieByDirector(inp[1]))
        else:
            print('Incorrect number of arguments')
    elif command == 'search by year':
        if len(inp) == 2:
            print(searchMovieByYear(inp[1]))
        else:
            print('Incorrect number of arguments')
    elif command == 'search by EIDR':
        if len(inp) == 2:
            print(searchMovieByEIDR(inp[1]))
        else:
            print('Incorrect number of arguments')
    elif command == 'sort by title':
        if len(inp) == 1:
            print(sortByTitle())
        else:
            print('Incorrect number of arguments')
    elif command == 'sort by director':
        if len(inp) == 1:
            print(sortByDirector())
        else:
            print('Incorrect number of arguments')
    elif command == 'sort by year':
        if len(inp) == 1:
            print(sortByYear())
        else:
            print('Incorrect number of arguments')
    elif command == 'sort by EIDR':
        if len(inp) == 1:
            print(sortByEIDR())
        else:
            print('Incorrect number of arguments')
    elif command == 'quit':
        break
    else:
        print('Invalid Command.')