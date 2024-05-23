import os
import mysql.connector
import datetime
from bs4 import BeautifulSoup

# Input: html_file = filename

def extract_text(html_file):

    f = open(html_file, errors='replace')
    content = ''
    html_content = f.read()
    parse = BeautifulSoup(html_content, 'lxml')
    find_p = parse.find_all('p')
    for tag in find_p:
        content = content + str({tag.text})
    f.close()
    return content

def to_int(string):
    new_string = string.replace(',','')
    number = int(new_string)
    return number

def to_date(date):
    new_date = datetime.datetime.strptime(date, "%d/%m/%Y %H:%M")
    return new_date
        
def create_table(db_name):
    db_name.execute("DROP TABLE IF EXISTS wikipedia")
    db_name.execute("""CREATE TABLE wikipedia (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        filetype VARCHAR(255),
                        last_modified DATETIME, 
                        size_bytes INT, 
                        filename VARCHAR(255), 
                        path VARCHAR(255), 
                        corpus LONGTEXT)""")
    

def remove_list(list):
    current_path = os.getcwd()
    path = current_path + '/' + list
    if os.path.exists(path):
        os.remove(path)

def get_tuple(line):
    mytuple = ()
    elements = line.split()
    if len(elements) != 0:
        if '/' in elements[0]:
            if (elements[3] not in ['.', '..', 'Queries']) & (elements[2] == '<DIR>'):
                mytuple = (elements[2], to_date(elements[0]+' '+elements[1]), 0, elements[3], incomplete_path+'\\'+elements[3], str('')) 
            elif (elements[3][-5:] == '.html') & (elements[2] != '<DIR>'):
                content = extract_text(incomplete_path+'\\'+elements[3])
                mytuple = (str('<FILE>'), to_date(elements[0]+' '+elements[1]), to_int(elements[2]), elements[3], incomplete_path+'\\'+elements[3], content)
        if len(mytuple)>0:   
            return mytuple
        
def create_index_name(cursor):
    query = "CREATE INDEX Idx_name ON wikipedia (filename) USING BTREE VISIBLE"
    cursor.execute(query)

def create_index_content(cursor):
    query = "CREATE FULLTEXT INDEX Idx_content ON wikipedia (corpus) VISIBLE"
    cursor.execute(query)

def get_occurrences_in_content(string, cursor):
    query = "SELECT (length(corpus)-length(REPLACE(corpus,'" + string + "','')))/length('" + string + "') FROM wikipedia"
    cursor.execute(query)

#def get_occurrences_in_content(string,cursor):



# Get the paths and filenames of all the directories and the files in the main directory 'DCR-B'

remove_list('list.txt')

os.system('dir /S>> list.txt') 

# list.txt format: -Directory path
# -Date   -Hour   -<DIR> or n.bytes (for files)   -Name (file or directory)
# -Subdirectory path
# ...

# Create the db


mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="TRuCeklAn15?",
  database="mydatabase"
)

mycursor = mydb.cursor()
#mycursor.execute("CREATE DATABASE mydatabase")
create_table(mycursor)

# Read 'list txt' and create the tuples to be inserted into the db

f = open('list.txt')
txt = f.readlines()
f.close()
i=0
j=0
k = len(txt)
batch_dim = 5
list_tuples = []

for line in txt:
    j+=1
    elements = line.split()
    if len(elements) != 0:
        if elements[0] == 'Directory':
            incomplete_path = elements[2]
        tuple = get_tuple(line)
        if tuple != None:
            val = tuple
            sql = """
                INSERT INTO wikipedia 
                (filetype, last_modified, size_bytes, filename, path, corpus) 
                VALUES (%s,%s,%s,%s,%s,%s)
                """
            list_tuples.append(tuple)
            i+=1
    if ((i==batch_dim) or (j==k)):
        print(list_tuples)
        mycursor.executemany(sql, list_tuples)
        mydb.commit()
        list_tuples = []
        i=0
            
# Creation of the indexes

create_index_name(mycursor)
create_index_content(mycursor)