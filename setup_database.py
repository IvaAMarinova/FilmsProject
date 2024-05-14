import sqlite3

def create_connection(db_file):
    """ Create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except sqlite3.Error as e:
        print(e)
    return conn

def create_table():
    """ Create tables in the SQLite database """
    conn = create_connection("mydatabase.db")
    if conn is not None:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS movies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                year TEXT,
                genre TEXT,
                description TEXT
            );
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS reviews (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                movie_id INTEGER,
                review TEXT,
                rating INTEGER,
                author TEXT,
                FOREIGN KEY (movie_id) REFERENCES movies (id)
            );
        ''')
        conn.commit()
        conn.close()

def add_movie(title, year, genre, description):
    """ Add a new movie to the movies table """
    conn = create_connection("mydatabase.db")
    sql = ''' INSERT INTO movies(title, year, genre, description)
              VALUES(?,?,?,?) '''
    cursor = conn.cursor()
    cursor.execute(sql, (title, year, genre, description))
    conn.commit()
    conn.close()

def add_review(movie_id, review, rating, author):
    """ Add a new review to the reviews table """
    conn = create_connection("mydatabase.db")
    sql = ''' INSERT INTO reviews(movie_id, review, rating, author)
              VALUES(?,?,?,?) '''
    cursor = conn.cursor()
    cursor.execute(sql, (movie_id, review, rating, author))
    conn.commit()
    conn.close()

def get_movies():
    """ Query all rows in the movies table """
    conn = create_connection("mydatabase.db")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM movies")
    rows = cursor.fetchall()
    for row in rows:
        print(row)
    conn.close()

def get_reviews():
    """ Query all rows in the reviews table """
    conn = create_connection("mydatabase.db")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM reviews")
    rows = cursor.fetchall()
    for row in rows:
        print(row)
    conn.close()

def update_movie(id, title, year, genre, description):
    """ Update a movie by movie id """
    conn = create_connection("mydatabase.db")
    sql = ''' UPDATE movies
              SET title = ?,
                  year = ?,
                  genre = ?,
                  description = ?
              WHERE id = ?'''
    cursor = conn.cursor()
    cursor.execute(sql, (title, year, genre, description, id))
    conn.commit()
    conn.close()

def delete_movie(id):
    """ Delete a movie by movie id """
    conn = create_connection("mydatabase.db")
    sql = 'DELETE FROM movies WHERE id=?'
    cursor = conn.cursor()
    cursor.execute(sql, (id,))
    conn.commit()
    conn.close()