import os
import sys
from dotenv import set_key, load_dotenv

# Define the ENV_PATH - match it with the one used in other scripts
ENV_PATH = "D:\\2Cents Cap\\DataFetcher\\zerodha\\.env"

# Credentials from zerodha_token_automation.py
API_KEY = "n3a1ly52x1d99ntm"
API_SECRET = "kdatn7jgpehykadplml2f91hxhsuk7ht"



# QuestDB credentials - match what's used in qdb.py
QUESTDB_HOST = "qdb3.satyvm.com"
QUESTDB_PORT = "443"
QUESTDB_USER = "2Cents"
QUESTDB_PASSWORD = "2Cents$1012cc"

print(f"Setting up .env file at: {ENV_PATH}")

# Ensure the directory exists
os.makedirs(os.path.dirname(ENV_PATH), exist_ok=True)

# Create .env if it doesn't exist
if not os.path.exists(ENV_PATH):
    print("Creating new .env file...")
    with open(ENV_PATH, 'w') as f:
        f.write("")
else:
    print("Updating existing .env file...")

# Load the existing .env file
load_dotenv(ENV_PATH)

# Set Zerodha API key and secret
set_key(ENV_PATH, "ZERODHA_API_KEY", API_KEY)
set_key(ENV_PATH, "ZERODHA_API_SECRET", API_SECRET)

# Set QuestDB credentials
set_key(ENV_PATH, "QUESTDB_HOST", QUESTDB_HOST)
set_key(ENV_PATH, "QUESTDB_PORT", QUESTDB_PORT)
set_key(ENV_PATH, "QUESTDB_USER", QUESTDB_USER)
set_key(ENV_PATH, "QUESTDB_PASSWORD", QUESTDB_PASSWORD)

# Check if access token is already set
access_token = os.getenv("ZERODHA_ACCESS_TOKEN")
if access_token:
    print(f"ZERODHA_ACCESS_TOKEN already exists in .env file")
else:
    print("ZERODHA_ACCESS_TOKEN is not set. You need to generate it.")
    print("Please follow these steps:")
    print("1. Run 'python listner.py' in one terminal")
    print("2. Run 'python zerodha_token_automation.py' in another terminal")
    print("3. Verify with 'python verify_token.py' before running zerodha_run_connector.py")

# Verify all required credentials
load_dotenv(ENV_PATH)
api_key = os.getenv("ZERODHA_API_KEY")
api_secret = os.getenv("ZERODHA_API_SECRET")
access_token = os.getenv("ZERODHA_ACCESS_TOKEN")
questdb_host = os.getenv("QUESTDB_HOST")
questdb_port = os.getenv("QUESTDB_PORT")
questdb_user = os.getenv("QUESTDB_USER")

print("\nCredentials status in .env file:")
print(f"ZERODHA_API_KEY: {'✅ SET' if api_key else '❌ NOT SET'}")
print(f"ZERODHA_API_SECRET: {'✅ SET' if api_secret else '❌ NOT SET'}")
print(f"ZERODHA_ACCESS_TOKEN: {'✅ SET' if access_token else '❌ NOT SET'}")
print(f"QUESTDB_HOST: {'✅ SET' if questdb_host else '❌ NOT SET'}")
print(f"QUESTDB_PORT: {'✅ SET' if questdb_port else '❌ NOT SET'}")
print(f"QUESTDB_USER: {'✅ SET' if questdb_user else '❌ NOT SET'}")

if api_key and api_secret:
    print("\n✅ Zerodha API credentials have been set successfully.")
    if not access_token:
        print("\nNext steps to generate an access token:")
        print("1. Run 'python listner.py' in one terminal")
        print("2. Run 'python zerodha_token_automation.py' in another terminal")
    else:
        print("\nAll Zerodha credentials are set.")

print("\nQuestDB Connection:")
print(f"The system will attempt to connect to QuestDB at {QUESTDB_HOST}:{QUESTDB_PORT}")
print(f"Using credentials: user={QUESTDB_USER}")
print("\nIf QuestDB connection fails, check the following:")
print("1. Make sure QuestDB server is running at the specified host and port")
print("2. Verify that the credentials are correct")
print("3. Check network connectivity to the QuestDB server")
print("\nTo run the connector:")
print("python zerodha_run_connector.py") 