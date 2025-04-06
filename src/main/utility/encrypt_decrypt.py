from cryptography.fernet import Fernet
import logging

logger = logging.getLogger(__name__)

class EncryptDecrypt:
    def __init__(self, key):
        self.cipher = Fernet(key)

    def encrypt(self, data):
        try:
            encrypted_data = self.cipher.encrypt(data.encode())
            logger.info("Data encrypted successfully")
            return encrypted_data
        except Exception as e:
            logger.error(f"Error encrypting data: {str(e)}")
            raise

    def decrypt(self, encrypted_data):
        try:
            decrypted_data = self.cipher.decrypt(encrypted_data).decode()
            logger.info("Data decrypted successfully")
            return decrypted_data
        except Exception as e:
            logger.error(f"Error decrypting data: {str(e)}")
            raise 