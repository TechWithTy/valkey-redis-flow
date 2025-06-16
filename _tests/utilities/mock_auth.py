"""
Mock utilities for Supabase client and auth service.
Used to replace Supabase dependencies in tests and when Supabase is not available.
"""

import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

class MockSupabaseClient:
    """Mock implementation of Supabase client"""
    
    def __init__(self, user_data: Optional[Dict[str, Any]] = None):
        self.user_data = user_data or {
            "id": "mock-user-id",
            "email": "mock-user@example.com",
            "role": "user"
        }
    
    async def auth(self):
        """Return mock auth API"""
        return self
    
    async def get_user(self, jwt: str = None):
        """Return mock user data"""
        if not jwt or jwt == "invalid":
            return None
        return self.user_data


class MockSupabaseAuthService:
    """Mock implementation of Supabase auth service"""
    
    def __init__(self, client=None):
        self.client = client or MockSupabaseClient()
        self._current_user = None
    
    async def get_current_user(self):
        """Return mock current user"""
        if self._current_user is None:
            self._current_user = {
                "id": "mock-user-id",
                "email": "mock-user@example.com",
                "role": "user"
            }
        return self._current_user
    
    def verify_jwt(self, token: str) -> bool:
        """Verify JWT token - returns True except for specific invalid tokens"""
        if not token or token == "invalid":
            return False
        return True
    
    def get_user_id(self, token: str) -> str:
        """Extract user ID from token"""
        if not token or token == "invalid":
            return "anonymous"
        return "mock-user-id"


async def get_mock_supabase_client():
    """Get a mock Supabase client instance"""
    return MockSupabaseClient()


async def get_mock_auth_service():
    """Get a mock Supabase auth service instance"""
    client = await get_mock_supabase_client()
    return MockSupabaseAuthService(client)