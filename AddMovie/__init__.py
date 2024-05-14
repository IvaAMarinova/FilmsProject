import logging
import azure.functions as func
import os
import json
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData
from sqlalchemy.orm import sessionmaker

DATABASE_URL = f"mysql+mysqlconnector://{os.getenv('DB_USER')}:" \
               f"{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}/" \
               f"{os.getenv('DB_NAME')}?ssl_mode={os.getenv('DB_SSL_MODE')}"

engine = create_engine(DATABASE_URL, echo=True)
metadata = MetaData()
movies = Table('Movies', metadata,
               Column('id', Integer, primary_key=True),
               Column('title', String(255)),
               Column('year', String(255)),
               Column('genre', String(255)),
               Column('description', String(1024)))

Session = sessionmaker(bind=engine)
session = Session()

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    
    try:
        req_body = req.get_json()
        title = req_body.get('title')
        year = req_body.get('year')
        genre = req_body.get('genre')
        description = req_body.get('description')

        if not all([title, year, genre, description]):
            raise ValueError("Please provide all movie details (title, year, genre, description)")

        new_movie = movies.insert().values(title=title, year=year, genre=genre, description=description)
        session.execute(new_movie)
        session.commit()
        
        return func.HttpResponse(f"Movie '{title}' added successfully!", status_code=200)
    
    except ValueError as ve:
        session.rollback()
        return func.HttpResponse(str(ve), status_code=400)
    
    except Exception as e:
        session.rollback()
        logging.error("An error occurred: %s", str(e))
        return func.HttpResponse(f"An error occurred: {str(e)}", status_code=500)
    
    finally:
        session.close()
