#!/usr/bin/env python3
"""
Helper script to get JWT token from Superset using session cookie.

Since your Superset instance uses Google OAuth, you need to:
1. Log in to Superset in your browser
2. Open Developer Tools > Application > Cookies
3. Copy the 'session' cookie value
4. Run this script with that cookie value
"""

import sys
import os
import httpx
from dotenv import load_dotenv

load_dotenv()

SUPERSET_BASE_URL = os.getenv("SUPERSET_BASE_URL", "http://localhost:8088")

def get_token_from_session(session_cookie: str):
    """Get JWT token using session cookie"""

    client = httpx.Client(base_url=SUPERSET_BASE_URL, timeout=30.0)
    from urllib.parse import urlparse
    domain = urlparse(SUPERSET_BASE_URL).netloc
    client.cookies.set("session", session_cookie, domain=domain)

    try:
        # First, try to get user info to verify session is valid
        response = client.get("/api/v1/me/")
        if response.status_code == 200:
            print("✓ Session cookie is valid!")
            user_data = response.json()
            print(f"✓ Logged in as: {user_data.get('username', 'unknown')}")

            # Now try to get or refresh the JWT token
            refresh_response = client.post("/api/v1/security/refresh")
            if refresh_response.status_code == 200:
                token_data = refresh_response.json()
                access_token = token_data.get("access_token")
                if access_token:
                    print(f"\n✓ JWT Access Token obtained!")
                    print(f"\nAdd this to your .env file:")
                    print(f"SUPERSET_JWT_TOKEN={access_token}")
                    return access_token
                else:
                    print("✗ No access_token in response")
            else:
                print(f"✗ Failed to refresh token: {refresh_response.status_code}")
                print(f"Response: {refresh_response.text}")
        else:
            print(f"✗ Session cookie is invalid: {response.status_code}")
            print(f"Response: {response.text}")
    except Exception as e:
        print(f"✗ Error: {e}")
    finally:
        client.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python get_token.py <session_cookie_value>")
        print("\nTo get your session cookie:")
        print("1. Open Superset in your browser")
        print("2. Open Developer Tools (F12)")
        print(f"3. Go to Application > Cookies > {SUPERSET_BASE_URL}")
        print("4. Copy the value of the 'session' cookie")
        print("5. Run: python get_token.py '<cookie_value>'")
        sys.exit(1)

    session_cookie = sys.argv[1]
    get_token_from_session(session_cookie)
