"""
Security utilities for encryption and session management
"""
import base64
import json
import os
from typing import Optional, Dict, Any
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from .config import settings


class SecurityManager:
    """Manages encryption and secure storage of sensitive data"""
    
    def __init__(self):
        self._fernet = None
        self._initialize_fernet()
    
    def _initialize_fernet(self):
        """Initialize Fernet cipher for encryption"""
        if not settings.session_enc_key:
            # Generate a new key if not provided
            key = Fernet.generate_key()
            print(f"Generated new encryption key: {base64.b64encode(key).decode()}")
            print("Please set SESSION_ENC_KEY in your .env file")
            return
        
        try:
            # Use provided key
            key = base64.b64decode(settings.session_enc_key)
            self._fernet = Fernet(key)
        except Exception as e:
            print(f"Error initializing encryption: {e}")
            self._fernet = None
    
    def encrypt_data(self, data: Dict[str, Any]) -> Optional[str]:
        """Encrypt data dictionary"""
        if not self._fernet:
            return None
        
        try:
            json_data = json.dumps(data, ensure_ascii=False)
            encrypted = self._fernet.encrypt(json_data.encode('utf-8'))
            return base64.b64encode(encrypted).decode('utf-8')
        except Exception as e:
            print(f"Encryption error: {e}")
            return None
    
    def decrypt_data(self, encrypted_data: str) -> Optional[Dict[str, Any]]:
        """Decrypt data to dictionary"""
        if not self._fernet:
            return None
        
        try:
            encrypted_bytes = base64.b64decode(encrypted_data.encode('utf-8'))
            decrypted = self._fernet.decrypt(encrypted_bytes)
            return json.loads(decrypted.decode('utf-8'))
        except Exception as e:
            print(f"Decryption error: {e}")
            return None
    
    def generate_session_key(self) -> str:
        """Generate a new session encryption key"""
        return base64.b64encode(Fernet.generate_key()).decode('utf-8')
    
    def hash_content(self, content: str) -> str:
        """Generate SHA-256 hash of content for deduplication"""
        import hashlib
        return hashlib.sha256(content.encode('utf-8')).hexdigest()
    
    def validate_token(self, token: str) -> bool:
        """Validate Telegram bot token format"""
        if not token:
            return False
        
        # Basic validation: should be numeric:alphanumeric format
        parts = token.split(':')
        if len(parts) != 2:
            return False
        
        try:
            int(parts[0])  # Bot ID should be numeric
            return len(parts[1]) > 0  # Token should not be empty
        except ValueError:
            return False
    
    def sanitize_filename(self, filename: str) -> str:
        """Sanitize filename for safe storage"""
        import re
        # Remove or replace unsafe characters
        sanitized = re.sub(r'[<>:"/\\|?*]', '_', filename)
        # Limit length
        if len(sanitized) > 255:
            sanitized = sanitized[:255]
        return sanitized
    
    def create_secure_directory(self, path: str) -> bool:
        """Create directory with secure permissions"""
        try:
            os.makedirs(path, mode=0o700, exist_ok=True)
            return True
        except Exception as e:
            print(f"Error creating secure directory {path}: {e}")
            return False


# Global security manager instance
security_manager = SecurityManager()
