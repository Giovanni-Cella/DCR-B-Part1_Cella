import mysql.connector
import pandas as pd
import warnings

warnings.filterwarnings("ignore")

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="TRuCeklAn15?",
  database="mydatabase"
)

mycursor = mydb.cursor()

def get_occurrences_in_content(string):
    query = """SELECT id, filetype, last_modified, size_bytes, filename, path, convert(((length(corpus)-length(REPLACE(corpus,'""" + string + """','')))/length('""" + string + """')), UNSIGNED)
                FROM wikipedia
                WHERE (((length(corpus)-length(REPLACE(corpus,'""" + string + """','')))/length('""" + string + """')>0) | (filename LIKE '%""" + string + """%'))"""
    df = pd.read_sql(query, mydb)
    df.rename(columns={"""convert(((length(corpus)-length(REPLACE(corpus,'""" + string + """','')))/length('""" + string + """')), UNSIGNED)""":"Occurrences"}, inplace=True)
    return df

search_word = input('String to be searched in the database: ')
a = get_occurrences_in_content(search_word)
if (len(a)>0):
  print(a)
else:
   print('The string was not found in any filename or html file.')