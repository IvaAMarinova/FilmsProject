import logging

import azure.functions as func


def add_review_to_db(movie_id, review, rating, author):
    """ Insert a new review into the reviews table """
    conn = create_connection("mydatabase.db")
    sql = ''' INSERT INTO reviews(movie_id, review, rating, author)
              VALUES(?,?,?,?) '''
    try:
        cursor = conn.cursor()
        cursor.execute(sql, (movie_id, review, rating, author))
        conn.commit()
    finally:
        if conn:
            conn.close()

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        req_body = req.get_json()
        movie_id = req_body.get('movie_id')
        review = req_body.get('review')
        rating = req_body.get('rating')
        author = req_body.get('author')

        if not all([movie_id, review, rating, author]):
            raise ValueError("Missing review data")

        add_review_to_db(movie_id, review, rating, author)
        return func.HttpResponse(f"Review added successfully for movie ID {movie_id}")

    except ValueError as ve:
        return func.HttpResponse(str(ve), status_code=400)
    except Exception as e:
        return func.HttpResponse(f"An error occurred: {str(e)}", status_code=500)
