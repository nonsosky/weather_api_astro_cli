import pandas as pd
import requests
import os
import logging
import dotenv
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError

# Load environment variables
dotenv.load_dotenv()

API_KEY = os.getenv("API_KEY")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PORT = os.getenv("DB_PORT")
DB_PASSWORD = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
BASE_URL = "http://api.weatherstack.com/current"
locations = ['Abuja', 'Lagos']

Base = declarative_base()

class Weather(Base):

    __tablename__ = 'new_weather'

    id = Column(Integer, primary_key=True)
    location = Column(String(255))
    temperature = Column(Float)
    wind_speed = Column(Float)
    pressure = Column(Float)
    humidity = Column(Float)
    local_time = Column(DateTime)
    time_zone = Column(String(255))
    longitude = Column(Float)
    latitude = Column(Float)
    weather_desc = Column(String(255))

def extract_data(**kwargs):
    #Ingestion Layer
    weather_data = []
    try:
        for location in locations:
            params = {
                'access_key': API_KEY,
                'query': location
            }
            response = requests.get(BASE_URL, params=params)
            if response.status_code == 200:
                data = response.json()
                weather_data.append(data)
    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        print("Error fetching data: {e}")
    finally:
        if len(weather_data) == 0:
            logging.error("No data fetched")
            print("No data fetched")
        else:
            logging.info("Data fetched successfully")
            print("Data fetched successfully")
    ti = kwargs['ti']
    ti.xcom_push(key='weather_data', value=weather_data)
    return weather_data


def transform_data(**kwargs):
    ti = kwargs['ti']
    weather_data = ti.xcom_pull(key='weather_data', task_ids='extract_weather_data')
    #Transformation Layer
    transformed_data = []
    try:
        for data in weather_data:
            transformed_data.append({
                "location": data['location']['name'],
                "temperature": data['current']['temperature'],
                "wind_speed": data['current']['wind_speed'],
                "pressure": data['current']['pressure'],
                "humidity": data['current']['humidity'],
                "local_time": data['location']['localtime'],
                "time_zone": data['location']['timezone_id'],
                "longitude": data['location']['lon'],
                "latitude": data['location']['lat'],
                "weather_desc": data['current']['weather_descriptions'][0]
            })
    except Exception as e:
        logging.error(f"Error transforming data: {e}")
        print(f"Error transforming data: {e}")
    finally:
        if len(transformed_data) == 0:
            logging.error("No data transformed")
            print("No data transformed")
        else:
            logging.info("Data transformed successfully")
            print("Data transformed successfully")

    transformed_data = pd.DataFrame(transformed_data)
    ti.xcom_push(key='transformed_data', value=transformed_data)
    return transformed_data
    

def load_data(**kwargs):
    # Loading Layer
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(key='transformed_data', task_ids='transform_weather_data')
    dotenv.load_dotenv()
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}')
    Session = sessionmaker(bind=engine)
    session = Session()
    Base.metadata.create_all(engine)

    try:
        data_to_insert = transformed_data.to_dict(orient='records')
        session.bulk_insert_mappings(Weather, data_to_insert)
        session.commit()
        logging.info("Data loaded successfully")
        print("Data loaded successfully")
    except SQLAlchemyError as e:
        session.rollback()
        logging.error(f"Error loading data: {e}")
        print(f"Error loading data: {e}")
    finally:
        session.close()
