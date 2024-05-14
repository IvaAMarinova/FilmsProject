import logging
import azure.functions as func
import sqlite3

def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except sqlite3.Error as e:
        print(e)
    return conn

def calculate_averages():
    """ Calculate average ratings and update the movies table """
    conn = create_connection("mydatabase.db")
    try:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT movie_id, AVG(rating) as avg_rating
            FROM reviews
            GROUP BY movie_id
        ''')
        averages = cursor.fetchall()

        for movie_id, avg_rating in averages:
            cursor.execute('''
                UPDATE movies
                SET average_rating = ?
                WHERE id = ?
            ''', (avg_rating, movie_id))
        conn.commit()
    finally:
        if conn:
            conn.close()

def main(mytimer: func.TimerRequest) -> None:
    logging.info('Python timer trigger function started calculating averages.')

    if mytimer.past_due:
        logging.info('The timer is past due!')

    calculate_averages()

    logging.info('Average ratings updated successfully.')
