import logging
from telethon import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
from dotenv import load_dotenv
from db_utils import save_data_to_db
from media_utils import download_media
from logging_utils import setup_logging
import os

# Load environment variables from .env
load_dotenv()

# Initialize the logger
logger = setup_logging()

# Your Telegram API credentials
api_id = os.getenv("TELEGRAM_API_ID")
api_hash = os.getenv("TELEGRAM_API_HASH")
phone = os.getenv("TELEGRAM_PHONE")

# Channels to scrape
channels = [
    "https://t.me/DoctorsET",
    "https://t.me/lobelia4cosmetics",
    "https://t.me/yetenaweg",
    "https://t.me/EAHCI"
]

# Initialize the Telegram client
client = TelegramClient(phone, api_id, api_hash)

async def scrape_channel_with_media(channel):
    """Scrapes messages and media from a Telegram channel and stores them in PostgreSQL."""
    await client.start()
    try:
        logger.info(f"Scraping channel: {channel}")
        entity = await client.get_entity(channel)
        history = await client(GetHistoryRequest(
            peer=entity,
            limit=10000,  # You can adjust the limit as needed
            offset_date=None,
            offset_id=0,
            max_id=0,
            min_id=0,
            add_offset=0,
            hash=0
        ))
        messages = history.messages

        # List to hold both message data and media data
        message_data_list = []

        for message in messages:
            # Extract text messages (whether or not they have media)
            message_data = {
                'message_id': message.id,
                'channel': channel,
                'date': message.date,
                'text': message.message if message.message else None,  # Handle if there's no text
                'media_path': None  # Initialize media_path as None
            }

            # Handle media
            if message.media:
                # Generate file path for the media
                file_path = f"media/{message.id}.jpg"

                # Download media and save the path
                await download_media(client, message.media, file_path)
                message_data['media_path'] = file_path  # Set the media path after download

            # Add the message data (with or without media) to the list
            message_data_list.append(message_data)

        # Save messages (with media paths, if any) to the database
        if message_data_list:
            logger.info(f"Saving {len(message_data_list)} items from {channel}")
            save_data_to_db(message_data_list)

        return message_data_list

    except Exception as e:
        logger.error(f"Error scraping channel {channel}: {e}")
    finally:
        await client.disconnect()

if __name__ == "__main__":
    with client:
        for channel in channels:
            client.loop.run_until_complete(scrape_channel_with_media(channel))