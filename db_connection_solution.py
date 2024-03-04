#-------------------------------------------------------------------------
# AUTHOR: your name
# FILENAME: title of the source file
# SPECIFICATION: description of the program
# FOR: CS 4250- Assignment #1
# TIME SPENT: how long it took you to complete the assignment
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
import psycopg2;
from psycopg2.extras import RealDictCursor

def connectDataBase():
    global conn

    # Create a database connection object using psycopg2
    DB_NAME = "CPP"
    DB_USER = "postgres"
    DB_PASS = "123"
    DB_HOST = "localhost"
    DB_PORT = "5432"

    try:
        conn = psycopg2.connect(database=DB_NAME,
                                user=DB_USER,
                                password=DB_PASS,
                                host=DB_HOST,
                                port=DB_PORT,
                                cursor_factory=RealDictCursor)
        
        # Creating tables if they don't exist
        cur = conn.cursor()

        # Create Categories table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Categories (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) UNIQUE
            )
        """)

        # Create Documents table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Documents (
                id SERIAL PRIMARY KEY,
                text TEXT,
                title VARCHAR(255),
                date DATE,
                category VARCHAR(100),
                FOREIGN KEY (category) REFERENCES Categories(name)
            )
        """)

        # Create Terms table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Terms (
                term VARCHAR(255) PRIMARY KEY,
                num_chars INT
            )
        """)

        # Create Inverted_Index table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Inverted_Index (
                term VARCHAR(255) REFERENCES Terms(term),
                doc_id INTEGER REFERENCES Documents(id),
                term_count INTEGER,
                PRIMARY KEY (term, doc_id)
            )
        """)

        conn.commit()
        return conn

    except psycopg2.Error as e:
        print("Database not connected successfully:", e)
        return None


def createCategory(cur, catId, catName):

    # Insert a category in the database
    try:
        # Inserting a category into the database
        cur.execute("INSERT INTO Categories (id, name) VALUES (%s, %s)", (catId, catName))
        conn.commit()
        print("Category created successfully")
    except Exception as e:
        print("Error creating category:", e)
    
def createDocument(cur, docId, docText, docTitle, docDate, docCat):

    # 1 Get the category id based on the informed category name
    # --> add your Python code here
    try: 
        cur.execute("SELECT id FROM Categories WHERE name = %s", (docCat,))
        cat_id = cur.fetchone()
        if cat_id:
            cat_id = cat_id['id']

    # 2 Insert the document in the database. For num_chars, discard the spaces and punctuation marks.
    # --> add your Python code here
            cur.execute("INSERT INTO Documents (id, text, title, date, category) VALUES (%s, %s, %s, %s, %s)",
                (docId, docText, docTitle, docDate, cat_id))
            conn.commit()
    # 3 Update the potential new terms.
    # 3.1 Find all terms that belong to the document. Use space " " as the delimiter character for terms and Remember to lowercase terms and remove punctuation marks.
    # 3.2 For each term identified, check if the term already exists in the database
    # 3.3 In case the term does not exist, insert it into the database
    # --> add your Python code here
            terms = [term.strip().lower() for term in docText.split()]
            terms = set(terms)

            for term in terms:
                cur.execute("INSERT INTO Terms (term) VALUES (%s) ON CONFLICT DO NOTHING", (term,))

    # 4 Update the index
    # 4.1 Find all terms that belong to the document
    # 4.2 Create a data structure the stores how many times (count) each term appears in the document
    # 4.3 Insert the term and its corresponding count into the database
    # --> add your Python code here
            for term in terms:
                cur.execute("INSERT INTO Inverted_Index (term, doc_id) VALUES (%s, %s) ON CONFLICT (term, doc_id) DO UPDATE SET term_count = Inverted_Index.term_count + 1",
                            (term, docId))

            conn.commit()
            print("Document created successfully")
        else:
            print("Category does not exist.")
    except psycopg2.Error as e:
        print("Error creating document:", e)

def deleteDocument(cur, docId):

    try:
        # 1. Query the index based on the document to identify terms
        cur.execute("SELECT term FROM Inverted_Index WHERE doc_id = %s", (docId,))
        terms = cur.fetchall()

        if terms:
            terms = [term['term'] for term in terms]

            # 1.1 For each term identified, delete its occurrences in the index for that document
            for term in terms:
                cur.execute("DELETE FROM Inverted_Index WHERE term = %s AND doc_id = %s", (term, docId))

                # 1.2 Check if there are no more occurrences of the term in another document.
                cur.execute("SELECT COUNT(*) FROM Inverted_Index WHERE term = %s", (term,))
                count = cur.fetchone()['count']
                if count == 0:
                    # If this happens, delete the term from the database.
                    cur.execute("DELETE FROM Terms WHERE term = %s", (term,))

            # 2. Delete the document from the database
            cur.execute("DELETE FROM Documents WHERE id = %s", (docId,))
            conn.commit()
            print("Document deleted successfully")
        else:
            print("Document does not exist.")
    except psycopg2.Error as e:
        print("Error deleting document:", e)

def updateDocument(cur, docId, docText, docTitle, docDate, docCat):
    try:
        # 1. Delete the document
        deleteDocument(cur, docId)

        # 2. Create the document with the same ID
        createDocument(cur, docId, docText, docTitle, docDate, docCat)

    except psycopg2.Error as e:
        print("Error updating document:", e)

def getIndex(cur):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    try:
        # Query the database to return the documents where each term occurs with their corresponding count
        cur.execute("SELECT term, doc_id, term_count FROM Inverted_Index")
        rows = cur.fetchall()

        inverted_index = {}
        for row in rows:
            term = row['term']
            doc_id = row['doc_id']
            term_count = row['term_count']

            if term in inverted_index:
                inverted_index[term].append(f"{doc_id}: {term_count}")
            else:
                inverted_index[term] = [f"{doc_id}: {term_count}"]

        return inverted_index
    except psycopg2.Error as e:
        print("Error getting inverted index:", e)
        return {}