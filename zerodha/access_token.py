from kiteconnect import KiteConnect
import os
from dotenv import load_dotenv

def generate_access_token(api_key, api_secret):
    try:
        # Initialize KiteConnect with API key
        kite = KiteConnect(api_key=api_key)

        # Generate login URL
        login_url = kite.login_url()
        print(f"Please visit this URL to authenticate: {login_url}")

        # Prompt user to log in and get the request token from the redirect URL
        request_token = input("Enter the request token from the URL after login: ")

        # Generate session and get access token
        data = kite.generate_session(request_token, api_secret=api_secret)
        access_token = data["access_token"]
        print(f"Access Token: {access_token}")

        # Optionally save to .env file
        with open('.env', 'a') as env_file:
            env_file.write(f'\nZERODHA_ACCESS_TOKEN={access_token}\n')
        print("Access token saved to .env file")

        return access_token
    except Exception as e:
        print(f"Error generating access token: {e}")
        return None

if __name__ == "__main__":
    # Load environment variables
    load_dotenv()

    # Get API key and secret from .env
    api_key = os.getenv('ZERODHA_API_KEY')
    api_secret = os.getenv('ZERODHA_API_SECRET')

    if not api_key or not api_secret:
        print("Error: ZERODHA_API_KEY and ZERODHA_API_SECRET must be set in .env")
    else:
        generate_access_token(api_key, api_secret)