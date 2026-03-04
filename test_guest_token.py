#!/usr/bin/env python3
"""
Test script to verify guest token generation

This script tests the guest token generation functionality and validates
that the token can be used to access Superset resources.
"""

import os
import sys
import asyncio
import httpx
from dotenv import load_dotenv
import jwt
from datetime import datetime, timedelta

# Load environment variables
load_dotenv()

SUPERSET_BASE_URL = os.getenv("SUPERSET_BASE_URL", "http://localhost:8088")
GUEST_TOKEN_JWT_SECRET = os.getenv("GUEST_TOKEN_JWT_SECRET")
GUEST_TOKEN_JWT_AUDIENCE = os.getenv("GUEST_TOKEN_JWT_AUDIENCE")
GUEST_ROLE_NAME = os.getenv("GUEST_ROLE_NAME")


def generate_test_token(dashboard_id: int, role: str = None):
    """Generate a test guest token"""
    if not GUEST_TOKEN_JWT_SECRET or not GUEST_TOKEN_JWT_AUDIENCE:
        print("❌ Missing guest token configuration")
        return None

    user = {
        "username": "test_user",
        "first_name": "Test",
        "last_name": "User"
    }

    # Add role if specified
    role_to_use = role or GUEST_ROLE_NAME
    if role_to_use:
        user["roles"] = [role_to_use]

    exp = datetime.utcnow() + timedelta(seconds=3600)
    payload = {
        "user": user,
        "resources": [{
            "type": "dashboard",
            "id": str(dashboard_id)
        }],
        "rls": [],
        "aud": GUEST_TOKEN_JWT_AUDIENCE,
        "exp": exp,
        "type": "guest"
    }

    token = jwt.encode(payload, GUEST_TOKEN_JWT_SECRET, algorithm="HS256")
    return token


async def test_guest_token_access(dashboard_id: int, role: str = None):
    """Test if the generated guest token can access a dashboard"""
    print(f"\n🧪 Testing Guest Token for Dashboard {dashboard_id}")
    print(f"   Role: {role or GUEST_ROLE_NAME or 'default'}")
    print("=" * 60)

    # Generate token
    token = generate_test_token(dashboard_id, role)
    if not token:
        return

    print(f"✓ Generated token: {token[:50]}...")

    # Decode token to show payload (for debugging)
    try:
        decoded = jwt.decode(token, GUEST_TOKEN_JWT_SECRET, algorithms=["HS256"], audience=GUEST_TOKEN_JWT_AUDIENCE)
        print(f"✓ Token payload validated:")
        print(f"  - User: {decoded['user']['username']}")
        print(f"  - Role: {decoded['user'].get('roles', ['none'])[0]}")
        print(f"  - Resource: {decoded['resources'][0]['type']} #{decoded['resources'][0]['id']}")
        print(f"  - Expires: {datetime.fromtimestamp(decoded['exp'])}")
    except Exception as e:
        print(f"❌ Token decode failed: {e}")
        return

    # Try to access the dashboard with the token
    async with httpx.AsyncClient(base_url=SUPERSET_BASE_URL, timeout=30.0) as client:
        try:
            # Test guest token endpoint
            response = await client.get(
                "/api/v1/security/guest_token/",
                headers={"Authorization": f"Bearer {token}"}
            )
            print(f"\n📡 Guest token validation: {response.status_code}")
            if response.status_code == 200:
                print("✓ Guest token is valid!")
            else:
                print(f"❌ Response: {response.text}")

            # Try to get dashboard info
            response = await client.get(
                f"/api/v1/dashboard/{dashboard_id}",
                headers={"Authorization": f"Bearer {token}"}
            )
            print(f"\n📊 Dashboard access: {response.status_code}")
            if response.status_code == 200:
                data = response.json()
                print(f"✓ Successfully accessed dashboard!")
                print(f"  - Title: {data.get('result', {}).get('dashboard_title', 'N/A')}")
            else:
                print(f"❌ Response: {response.text[:200]}")

        except Exception as e:
            print(f"❌ Error testing token: {e}")


if __name__ == "__main__":
    dashboard_id = int(sys.argv[1]) if len(sys.argv) > 1 else 432
    role = sys.argv[2] if len(sys.argv) > 2 else None

    print(f"🔧 Configuration:")
    print(f"   Base URL: {SUPERSET_BASE_URL}")
    print(f"   JWT Secret: {'✓ Set' if GUEST_TOKEN_JWT_SECRET else '❌ Missing'}")
    print(f"   JWT Audience: {GUEST_TOKEN_JWT_AUDIENCE or '❌ Missing'}")
    print(f"   Default Role: {GUEST_ROLE_NAME or 'None'}")

    asyncio.run(test_guest_token_access(dashboard_id, role))
