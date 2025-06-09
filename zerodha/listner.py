from flask import Flask, jsonify, request
import os
import logging
from dotenv import load_dotenv, set_key
from kiteconnect import KiteConnect
from urllib.parse import parse_qs
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables or set defaults from zerodha_token_automation.py
API_KEY = "n3a1ly52x1d99ntm"
API_SECRET = "kdatn7jgpehykadplml2f91hxhsuk7ht"
ENV_PATH = Path(__file__).resolve().parent.parent / '.env'

app = Flask(__name__)

def generate_access_token(request_token):
    """Generate access token using KiteConnect API"""
    try:
        logger.info(f"Generating access token with request token: {request_token}")
        kite = KiteConnect(api_key=API_KEY)
        data = kite.generate_session(request_token, api_secret=API_SECRET)
        access_token = data["access_token"] # type:ignore
        logger.info(f"Access token generated successfully: {access_token[:5]}...")
        
        # Save to .env file
        os.makedirs(os.path.dirname(ENV_PATH), exist_ok=True)
        
        # Create .env if it doesn't exist
        if not os.path.exists(ENV_PATH):
            with open(ENV_PATH, 'w') as f:
                f.write("")
        
        # Update the access token and API key
        load_dotenv(ENV_PATH)
        set_key(ENV_PATH, "ZERODHA_ACCESS_TOKEN", access_token)
        set_key(ENV_PATH, "ZERODHA_API_KEY", API_KEY)
        set_key(ENV_PATH, "ZERODHA_API_SECRET", API_SECRET)
        logger.info(f"Updated access token and API key in {ENV_PATH}")
        
        return access_token
    except Exception as e:
        logger.error(f"Error generating access token: {str(e)}")
        return None

# Endpoint to handle the redirect with request token
@app.route('/generate_token', methods=['GET'])
def handle_token():
    try:
        # Extract request token from URL parameters
        request_token = request.args.get('request_token')
        
        # If request_token is not directly available, try to extract from other parameters
        if not request_token:
            logger.info("Direct request_token not found, checking status parameter...")
            status = request.args.get('status')
            if status and 'request_token=' in status:
                request_token = status.split('request_token=')[-1]
        
        if not request_token:
            # Final fallback - try parsing all query parameters
            all_params = request.query_string.decode('utf-8')
            logger.info(f"Checking all query parameters: {all_params}")
            if 'request_token=' in all_params:
                request_token = all_params.split('request_token=')[-1].split('&')[0]
        
        if not request_token:
            logger.error("No request token found in URL")
            return jsonify({'error': 'No request token found'}), 400
        
        logger.info(f"Request token extracted: {request_token}")
        
        # Generate access token
        access_token = generate_access_token(request_token)
        
        if access_token:
            return jsonify({
                'message': 'Access token generated successfully',
                'request_token': request_token,
                'access_token': access_token[:5] + '...'  # Only show first 5 chars for security
            })
        else:
            return jsonify({'error': 'Failed to generate access token'}), 500
            
    except Exception as e:
        logger.error(f"Error in handle_token: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/', methods=['GET'])
def home():
    return """
    <html>
        <body>
            <h1>Zerodha Token Handler</h1>
            <p>This server is running and waiting for the redirect from Zerodha authentication.</p>
        </body>
    </html>
    """

if __name__ == '__main__':
    logger.info("Starting listener on port 8080...")
    app.run(debug=True, port=8080)
