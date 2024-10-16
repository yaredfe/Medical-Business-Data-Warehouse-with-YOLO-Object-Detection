import logging

logger = logging.getLogger(__name__)

async def download_media(client, media, file_path):
    """Downloads media (images) from a Telegram message."""
    try:
        if media:
            await client.download_media(media, file_path)
            logger.info(f"Media downloaded to {file_path}")
        else:
            logger.info("No media to download")
    except Exception as e:
        logger.error(f"Error downloading media: {e}")