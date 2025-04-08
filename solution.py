# Homework for CS 50024, Unit 1
# Submitted by Michael Yudanin

import pandas as pd
import sqlite3
import csv
import math

# Part 1: Cleaning the existing files, and creating new files --------------------------------------

# Read the CSV files, replace \N with math.nan, create new files
files = ['cast', 'titles', 'people']
for f in files:
    df = pd.read_csv(f'{f}.csv', encoding='utf-8') # preserves special characters
    df.replace(r'\\N', math.nan, regex=True, inplace=True)  # Replace \N with math.nan 
    df.to_csv(f'{f}_cleaned.csv', index=False, encoding='utf-8') # preserves special characters

# Create categories file
df = pd.read_csv('cast_cleaned.csv', encoding='utf-8')
categories = sorted(df['category'].dropna().unique())
category_df = pd.DataFrame({
    'CATEGORY_ID': range(1, len(categories) + 1),  # IDs from 1 to the number of categories
    'CATEGORY_NAME': categories
})
category_df.to_csv('category.csv', index=False, encoding='utf-8')

# In cast_cleaned.csv, replace category string with category ID
df = pd.read_csv('cast_cleaned.csv', encoding='utf-8')
df['category'] = df['category'].replace(categories, category_df['CATEGORY_ID'])
df.to_csv('cast_updated.csv', index=False, encoding='utf-8')


# Part 2: Create a database file, imdb.db, using the files cleaned and created ---------------------

con = sqlite3.connect('imdb.db')
con.execute('PRAGMA encoding="UTF-8";')
cur = con.cursor()

# Create tables
#   “titles” with columns from the titles.csv
#   “productions” with columns from the productions.csv
#   “ratings” with columns from the ratings.csv
#   “people” with columns from the people.csv
#   “cast” with columns from the cast.csv

# The tables will be created with the data from the respective files.
# Column names will be more meaningful
# Primary and foreign keys will be added

files = {
  'Table': ['titles', 'productions', 'ratings', 'people', 'cast'],
  'Columns': [[['titleId','TEXT'], ['ordering','INTEGER'], ['title','TEXT'], ['region','TEXT'], ['language','TEXT'], ['isOriginalTitle','INTEGER']],
    [['titleId','TEXT'], ['titleType','TEXT'], ['primaryTitle','TEXT'], ['originalTitle','TEXT'], ['startYear','INTEGER'], ['endYear','INTEGER'], ['runtimeMinutes','INTEGER'], ['genres','TEXT']],
    [['titleId','TEXT'], ['averageRating','REAL'], ['numVotes','INTEGER']],
    [['personId','TEXT'], ['primaryName','TEXT'], ['birthYear','INTEGER'], ['deathYear','INTEGER'], ['primaryProfession','TEXT'], ['knownForTitles','INTEGER']],
    [['titleId','TEXT'], ['ordering','INTEGER'], ['personId','TEXT'], ['category_id','INTEGER'], ['job','TEXT'], ['characters','TEXT']]
    ],
  'PrimaryKey': ['(titleId, ordering)', '(titleId)', '(titleId)', '(personId)', '(titleId, ordering)'],
  'ForeignKey': ['', '', '', 'FOREIGN KEY (personId) REFERENCES people', 'FOREIGN KEY (titleId) REFERENCES titles, FOREIGN KEY (personId) REFERENCES people'],
}

def create_table(r):
  '''
  1. Reads CSV file as specified in the input dataframe row
  2. Creates database table based on the definitions in the input dataframe raw
  3. Inserts data from the CSV file to the newly created table 
  '''
  with open (f"{r['Table']}.csv", 'r') as file:
    dr = csv.DictReader(file, delimiter = ',')
    file_column_names = dr.fieldnames
    to_db = [tuple(math.nan if row[col] == '\\N' else row[col] for col in file_column_names) for row in dr] # replace \N with math.nan
    
    # Drop table if exists
    drop_table_sql = f"DROP TABLE IF EXISTS {r['Table']};"
    cur.execute(drop_table_sql)

    # Create table
    create_table_sql = f"CREATE TABLE {r['Table']} ({', '.join([f'{col[0]} {col[1]}' for col in r['Columns']])}, PRIMARY KEY {r['PrimaryKey']}{', ' + r['ForeignKey'] if r['ForeignKey'] and r['ForeignKey'].strip() else ''});"
    cur.execute(create_table_sql)
    
    # Insert data into table
    placeholders = ', '.join(['?' for _ in file_column_names])
    insert_sql = f"INSERT INTO {r['Table']} VALUES ({placeholders});"
    cur.executemany(insert_sql, to_db)


# Run for all tables   
df_files = pd.DataFrame(files) 
df_files.apply(create_table, axis=1)

con.commit()
