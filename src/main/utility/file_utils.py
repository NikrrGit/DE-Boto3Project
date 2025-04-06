import os
import logging

logger = logging.getLogger(__name__)

def clean_up_local_files(directories):
    try:
        for directory in directories:
            if os.path.exists(directory):
                for root, dirs, files in os.walk(directory, topdown=False):
                    for name in files:
                        os.remove(os.path.join(root, name))
                    for name in dirs:
                        os.rmdir(os.path.join(root, name))
                os.rmdir(directory)
                logger.info(f"Cleaned up directory: {directory}")
            else:
                logger.warning(f"Directory not found: {directory}")
    except Exception as e:
        logger.error(f"Error cleaning up files: {str(e)}")
        raise 