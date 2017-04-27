import cassandra
from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect()

KEYSPACE = 'streaming'
IDENTIFIER = 1

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

def createStatusTable():
    session.execute("""drop table if EXISTS status""")
    session.execute("""create table status(identifier text PRIMARY KEY, username text, EIDR text, duration text)""")

createStatusTable()

def identifierValue():
    session.execute("""SELECT count(*) from streaming.status""")

def addRentalData():
    global IDENTIFIER
    session.execute("insert into status(identifier, username, EIDR, duration) VALUES (%s, %s, %s, %s)",
                    (str(IDENTIFIER), 'peruman', '3', '5'))
    IDENTIFIER = IDENTIFIER + 1

addRentalData()

# Rental Stuff
def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def checkMovie(EIDR, username, duration):
    global IDENTIFIER
    ret = session.execute("""SELECT eidr from streaming.movies where eidr='{0}'""".format(EIDR))
    selectedEIDR = -1
    for item in ret:
        # print(item.eidr)
        selectedEIDR = item.eidr

    if selectedEIDR == -1:
        print("There is no movie with that EIDR")
        return False

    ret = session.execute("""SELECT username from streaming.users where username='{0}'""".format(username))
    selectedUsername = "Admin"
    for item in ret:
        selectedUsername = item.username

    if selectedUsername == "Admin":
        print("There is no user with that username")
        return False

    if not is_number(duration):
        print("Duration is not a number of days")
        return False

    ret = session.execute("""Select eidr from streaming.status where username='{0}' ALLOW FILTERING""".format(username))
    rentalList = []
    for item in ret:
        rentalList.append(item.eidr)

    for thing in rentalList:
        if EIDR == thing:
            print("You have already rented or purchased that")
            return False

    return True

def rentMovie(EIDR, username, duration):
    global IDENTIFIER

    if checkMovie(EIDR, username, duration):
        session.execute("""INSERT into status(identifier, username, EIDR, duration) VALUES (%s, %s, %s, %s)""", (str(IDENTIFIER), username, EIDR, duration))
        IDENTIFIER = IDENTIFIER + 1
        return
    else:
        print("Something went wrong")
        return



def buyMovie(EIDR, username):
    global IDENTIFIER

    #Duration = -10 means the movie was purchased
    if checkMovie(EIDR, username, '-10'):
        session.execute("""INSERT into status(identifier, username, EIDR, duration) VALUES (%s, %s, %s, %s)""", (str(IDENTIFIER), username, EIDR, '-10'))
        IDENTIFIER = IDENTIFIER + 1
        return
    else:
        print("Something went wrong")
        return

def moviesRentedByUser(username):
    ret = session.execute("""Select eidr, duration from streaming.status where username='{0}' ALLOW FILTERING""".format(username))
    EIDRlist = []
    durationList = []

    for item in ret:
        EIDRlist.append(item.eidr)
        # print(item.duration)
        # if int(item.duration) != -10:
        durationList.append(item.duration)

    i = 0
    while i < len(durationList):
        if int(durationList[i]) == -10:
            del EIDRlist[i]
        i = i + 1

    for thing in EIDRlist:
        answer = session.execute("""Select * from streaming.movies where eidr='{0}'""".format(thing))
        print(answer.current_rows)

def moviesOwnedByUser(username):
    
    return


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
            searchMovieByTitle(inp[1])
        else:
            print('Incorrect number of arguments')
    elif command == 'search by director':
        if len(inp) == 2:
            searchMovieByDirector(inp[1])
        else:
            print('Incorrect number of arguments')
    elif command == 'search by year':
        if len(inp) == 2:
            searchMovieByYear(inp[1])
        else:
            print('Incorrect number of arguments')
    elif command == 'search by EIDR':
        if len(inp) == 2:
            searchMovieByEIDR(inp[1])
        else:
            print('Incorrect number of arguments')
    elif command == 'sort by title':
        if len(inp) == 1:
            sortByTitle()
        else:
            print('Incorrect number of arguments')
    elif command == 'sort by director':
        if len(inp) == 1:
            sortByDirector()
        else:
            print('Incorrect number of arguments')
    elif command == 'sort by year':
        if len(inp) == 1:
            sortByYear()
        else:
            print('Incorrect number of arguments')
    elif command == 'sort by EIDR':
        if len(inp) == 1:
            sortByEIDR()
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
            searchUserByName(inp[1])
        else:
            print('Incorrect number of arguments')
    elif command == 'search users by username':
        if len(inp) == 2:
            searchUserByUsername(inp[1])
        else:
            print('Incorrect number of arguments')
    elif command == 'search users by phone':
        if len(inp) == 2:
            searchUserByPhone(inp[1])
        else:
            print('Incorrect number of arguments')
    elif command == 'rent':
        if len(inp) == 4:
            rentMovie(inp[1], inp[2], inp[3])
        else:
            print('Incorrect number of arguments')
    elif command == 'buy':
        if len(inp) == 3:
            buyMovie(inp[1], inp[2])
        else:
            print('Incorrect number of arguments')
    elif command == 'movies rented by':
        if len(inp) == 2:
            moviesRentedByUser(inp[1])
        else:
            print('Incorrect number of arguments')
    elif command == 'quit':
        break
    else:
        print('Invalid Command.')
