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
    session.execute("insert into movies(title, director, year, EIDR) VALUES (%s, %s, %s, %s)",
                    ('Schindlers list', 'Steven Spielberg', '1993', '1'))
    session.execute("insert into movies(title, director, year, EIDR) VALUES (%s, %s, %s, %s)",
                    ('Inception', 'Chrisopher Nolan', '2010', '2'))
    session.execute("insert into movies(title, director, year, EIDR) VALUES (%s, %s, %s, %s)",
                    ('Up', 'Pete Doctor', '2009', '3'))


printSomething()

def addMovie(title, director, year, EIDR):
    print("Add Movie")
    session.execute("insert into movies(title, director, year, EIDR) VALUES (%s, %s, %s, %s)",
                    (title, director, year, EIDR))


def removeMovie(EIDR):
    print("Remove Movie")
    session.execute("DELETE FROM movies where EIDR = %s", EIDR)


def editMovie(title, director, year, EIDR):
    print("Edit Movie")
    # ret = session.execute("Select title, director, year, EIDR from movies where EIDR=%s", EIDR)
    # print(ret.current_rows)
    # for item in ret:
    #     print (item.title)
    session.execute("UPDATE movies SET title=%s, director=%s, year=%s where EIDR=%s IF EXISTS",
                    (title, director, year, EIDR))


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


class Movie_Row:
    """
        Written by the man, the myth, the legend, Stephen Nicholas Trotta the Third
    """

    def __init__(self, row, sort_by='title'):
        self.row = row
        self.sort_column = sort_by

    def __cmp__(self, other):
        if 'title' == self.sort_column:
            if self.row.title < other.get_title():
                return -1
            elif self.row.title == other.get_title():
                return 0
            else:
                return 1
        elif 'director' == self.sort_column:
            if self.row.director < other.get_director():
                return -1
            elif self.row.director == other.get_director():
                return 0
            else:
                return 1
        elif 'year' == self.sort_column:
            if self.row.year < other.get_year():
                return -1
            elif self.row.year == other.get_year():
                return 0
            else:
                return 1
        elif 'eidr' == self.sort_column:
            if self.row.eidr < other.get_eidr():
                return -1
            elif self.row.eidr == other.get_eidr():
                return 0
            else:
                return 1

    def set_sort_column(self, new_column):
        self.sort_column = new_column

    def get_title(self):
        return self.row.title

    def get_director(self):
        return self.row.director

    def get_year(self):
        return self.row.year

    def get_eidr(self):
        return self.row.eidr

    def get_row(self):
        return self.row


def sortByTitle():
    print("Sort by title")
    ret = session.execute("""Select * from streaming.movies""")

    allTitles = []
    for item in ret:
        allTitles.append(Movie_Row(item, 'title'))

    sortedTitles = sorted(allTitles)

    for item_obj in sortedTitles:
        print str(item_obj.get_title()), str(item_obj.get_director()), str(item_obj.get_year())


def sortByDirector():
    print("Sort by title")
    ret = session.execute("""Select * from streaming.movies""")

    allTitles = []
    for item in ret:
        allTitles.append(Movie_Row(item, 'director'))

    sortedTitles = sorted(allTitles)

    for item_obj in sortedTitles:
        print str(item_obj.get_title()), str(item_obj.get_director()), str(item_obj.get_year())


def sortByYear():
    print("Sort by title")
    ret = session.execute("""Select * from streaming.movies""")

    allTitles = []
    for item in ret:
        allTitles.append(Movie_Row(item, 'year'))

    sortedTitles = sorted(allTitles)

    for item_obj in sortedTitles:
        print str(item_obj.get_title()), str(item_obj.get_director()), str(item_obj.get_year())


def sortByEIDR():
    print("Sort by title")
    ret = session.execute("""Select * from streaming.movies""")

    allTitles = []
    for item in ret:
        allTitles.append(Movie_Row(item, 'eidr'))

    sortedTitles = sorted(allTitles)

    for item_obj in sortedTitles:
        print str(item_obj.get_title()), str(item_obj.get_director()), str(item_obj.get_year())

"""USER STUFF"""
# session.execute("""CREATE TYPE cc(card text, name text, csc text, date text)""")

session.execute("""drop table if EXISTS users""")
session.execute("""create table users(name text, username text PRIMARY KEY, phone text, address text)""")

def addMoreData():
    session.execute("insert into users(name, username, phone, address) VALUES (%s, %s, %s, %s)",
                    ('Nithin Perumal', 'peruman', '4083688244', '1652 Vireo Avenue'))

addMoreData()

def addUser(name, username, phone, address):
    session.execute("insert into users(name, username, phone, address) VALUES (%s, %s, %s, %s)",
                    (name, username, phone, address))

def removeUser(username):
    print("remove user")
    session.execute("""DELETE FROM users where username = '{0}'""".format(username))

def editUser(name, username, phone, address):
    print("Edit User")
    session.execute("UPDATE users SET name='{0}', address='{1}', phone='{2}' where username='{3}' IF EXISTS".format(name, address, phone, username))

def searchUserByName(name):
    print("Search user by name")
    ret = session.execute("""SELECT * from streaming.users where name='{0}' ALLOW FILTERING""".format(name))
    print(ret.current_rows)

def searchUserByUsername(username):
    print("Search user by username")
    ret = session.execute("""SELECT * from streaming.users where username='{0}' ALLOW FILTERING""".format(username))
    print(ret.current_rows)

def searchUserByPhone(phone):
    print("Search user by name")
    ret = session.execute("""SELECT * from streaming.users where phone='{0}' ALLOW FILTERING""".format(phone))
    print(ret.current_rows)


while True:
    inp = raw_input('command?').lstrip().split(',')
    i = 0
    for i in range(len(inp)):
        inp[i] = inp[i].strip()
    command = inp[0]
    if command == 'print something':
        printSomething()
        addMoreData()
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
    elif command == 'add user':
        if len(inp) == 5:
            addUser(inp[1], inp[2], inp[3], inp[4])
        else:
            print('Incorrect number of arguments')
    elif command == 'remove user':
        if len(inp) == 2:
            removeUser(inp[1])
        else:
            print('Incorrect number of arguments')
    elif command == 'edit user':
        if len(inp) == 5:
            editUser(inp[1], inp[2], inp[3], inp[4])
        else:
            print('Incorrect number of arguments')
    elif command == 'search users by name':
        if len(inp) == 2:
            print(searchUserByName(inp[1]))
        else:
            print('Incorrect number of arguments')
    elif command == 'search users by username':
        if len(inp) == 2:
            print(searchUserByUsername(inp[1]))
        else:
            print('Incorrect number of arguments')
    elif command == 'search users by phone':
        if len(inp) == 2:
            print(searchUserByPhone(inp[1]))
        else:
            print('Incorrect number of arguments')
    elif command == 'quit':
        break
    else:
        print('Invalid Command.')
