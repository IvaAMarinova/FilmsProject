import logging
import azure.functions as func
import sqlite3
import json

def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except sqlite3.Error as e:
        print(e)
    return conn

def search_movies(title=None):
    """ Search movies by title or return all if no title is provided """
    conn = create_connection("mydatabase.db")
    try:
        cursor = conn.cursor()
        if title:
            cursor.execute("SELECT * FROM movies WHERE title LIKE ?", ('%' + title + '%',))
        else:
            cursor.execute("SELECT * FROM movies")
        rows = cursor.fetchall()
        return rows
    finally:
        if conn:
            conn.close()

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request for searching movies.')

    title = req.params.get('title')
    try:
        movies = search_movies(title)
        movies_list = [{'id': row[0], 'title': row[1], 'year': row[2], 'genre': row[3], 'description': row[4], 'average_rating': row[5]} for row in movies]
        return func.HttpResponse(json.dumps(movies_list), mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(f"An error occurred: {str(e)}", status_code=500)
