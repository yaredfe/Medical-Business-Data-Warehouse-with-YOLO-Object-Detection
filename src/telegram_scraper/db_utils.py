import os
import psycopg2
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# PostgreSQL database credentials
db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")

logger = logging.getLogger(__name__)

def connect_to_db():
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host=db_host,
            dbname=db_name,
            user=db_user,
            password=db_password
        )
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        return None

def save_data_to_db(data):
    """Inserts scraped data (messages and media) into the PostgreSQL database."""
    conn = connect_to_db()
    if not conn:
        return
    
    cursor = conn.cursor()

    for message in data:
        try:
            # Insert message into the telegram_message table
            cursor.execute("""
                INSERT INTO message (message_id, channel, date, text)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (message_id) DO NOTHING
            """, (
                message['message_id'],
                message['channel'], 
                message['date'], 
                message.get('text')  # Use .get to avoid KeyError if 'text' is not present 
            ))

            # If media_path is not None or empty, insert it into the media table
            media_path = message.get('media_path')
            if media_path:
                cursor.execute("""
                    INSERT INTO media (message_id, media_path, channel, date)
                    VALUES (%s, %s, %s, %s)
                """, (
                    message['message_id'],
                    message["media_path"],
                    message['channel'], 
                    message['date'] 
                ))

        except Exception as e:
            logger.error(f"Error saving message {message['message_id']}: {e}") 
            conn.rollback()

    # Commit changes to the database
    conn.commit()
    
    # Close cursor and connection
    cursor.close()
    conn.close()
    
    logger.info("Data successfully saved to PostgreSQL")