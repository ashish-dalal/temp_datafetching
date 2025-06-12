import time
import pyotp
import asyncio
from playwright.async_api import async_playwright, TimeoutError
from kiteconnect import KiteConnect
from dotenv import set_key, load_dotenv
import os
import logging
from urllib.parse import urlparse, parse_qs
from pathlib import Path




# --- CONFIGURATION ---
ZERODHA_USER_ID = "VND352" 
ZERODHA_PASSWORD = "Sdollars$03"  
TOTP_SECRET = "YCIVQD3ZXKTKTZDAZBIOPOBVO3HWUWKO"  
API_KEY = "n3a1ly52x1d99ntm"  
API_SECRET = "kdatn7jgpehykadplml2f91hxhsuk7ht"  
REDIRECT_URL = "https://localhost:8080/" 
ENV_PATH = Path(__file__).resolve().parent.parent / '.env'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("zerodha_token_automation")

def get_fresh_totp():
    """Get a fresh TOTP with maximum remaining validity"""
    totp = pyotp.TOTP(TOTP_SECRET)
    # Get current timestamp
    timestamp = time.time()
    # Calculate seconds until next 30-second window
    seconds_to_wait = 30 - (int(timestamp) % 30)
    if seconds_to_wait < 5:  # If we're close to the end of the window, wait for next one
        time.sleep(seconds_to_wait)
    return totp.now()

def extract_request_token(url):
    """Extract request token from URL regardless of protocol"""
    try:
        # Parse the URL
        parsed = urlparse(url)
        # Get the query parameters
        query_params = parse_qs(parsed.query)
        # Extract request_token
        request_token = query_params.get("request_token", [None])[0]
        if not request_token:
            # Try looking for it in the status parameter
            status_param = query_params.get("status", [None])[0]
            if status_param and "request_token" in status_param:
                request_token = status_param.split("request_token=")[-1]
        if not request_token:
            raise ValueError("No request_token found in URL")
        return request_token
    except Exception as e:
        raise Exception(f"Failed to extract request token from URL: {str(e)}")

async def wait_for_element(page, selector, timeout=10000):
    """Wait for an element to be visible and clickable"""
    try:
        element = await page.wait_for_selector(selector, state="visible", timeout=timeout)
        if not element:
            raise TimeoutError(f"Element {selector} not found")
        return element
    except Exception as e:
        logger.error(f"Error waiting for element {selector}: {e}")
        await page.screenshot(path=f"error_{selector.replace('[', '_').replace(']', '_')}.png")
        raise

async def try_click(page, selector, retries=3, delay=1000):
    """Try to click an element multiple times"""
    for attempt in range(retries):
        try:
            element = await wait_for_element(page, selector)
            await element.click()
            return True
        except Exception as e:
            if attempt == retries - 1:
                logger.error(f"Failed to click {selector} after {retries} attempts: {e}")
                raise
            await page.wait_for_timeout(delay)
    return False

async def get_request_token():
    async with async_playwright() as p:
        # Launch browser with slower default timeout
        browser = await p.chromium.launch(headless=False)  # Set headless=True in production
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        
        try:
            # Set default timeout for all operations
            page.set_default_timeout(60000)  # 60 seconds
            
            login_url = f"https://kite.trade/connect/login?api_key={API_KEY}&v=3"
            logger.info(f"Navigating to {login_url}")
            await page.goto(login_url)
            await page.wait_for_load_state("networkidle")
            
            # Take screenshot of initial page
            await page.screenshot(path="initial_page.png")
            
            # Login step 1: Username and Password
            logger.info("Entering username and password...")
            
            # Wait for and fill username field
            userid_input = await wait_for_element(page, "#userid")
            await userid_input.fill(ZERODHA_USER_ID)
            await page.wait_for_timeout(1000)  # Wait for 1 second
            
            # Wait for and fill password field
            password_input = await wait_for_element(page, "#password")
            await password_input.fill(ZERODHA_PASSWORD)
            await page.wait_for_timeout(1000)  # Wait for 1 second
            
            # Take screenshot before clicking login
            await page.screenshot(path="before_login.png")
            
            # Try different selectors for the login button
            login_selectors = [
                "button[type='submit']",
                "button:has-text('Login')",
                ".button-orange"
            ]
            
            login_clicked = False
            for selector in login_selectors:
                try:
                    if await try_click(page, selector):
                        login_clicked = True
                        break
                except:
                    continue
            
            if not login_clicked:
                raise Exception("Could not click login button with any selector")
            
            await page.wait_for_load_state("networkidle")
            await page.wait_for_timeout(2000)  # Additional wait after login
            
            # Take screenshot after login
            await page.screenshot(path="after_login.png")
            
            # Login step 2: TOTP
            logger.info("Waiting for TOTP input field...")
            
            # Try different selectors for TOTP input
            totp_selectors = [
                "input[label='External TOTP']",
                "input[placeholder='TOTP']",
                "input[type='text'][autocomplete='off']",
                "input[placeholder*='TOTP']",
                "input.totp"
            ]
            
            totp_input = None
            for selector in totp_selectors:
                try:
                    totp_input = await wait_for_element(page, selector)
                    if totp_input:
                        break
                except:
                    continue
            
            if not totp_input:
                await page.screenshot(path="totp_not_found.png")
                raise Exception("Could not find TOTP input field with any selector")
            
            # Get a fresh TOTP
            totp = get_fresh_totp()
            logger.info(f"Generated TOTP code: {totp}")
            
            # Enter TOTP
            await totp_input.fill(totp)
            await page.wait_for_timeout(3000)
            logger.info("TOTP entered, waiting for automatic redirect...")
            await page.wait_for_load_state("networkidle")  # Wait for the page to finish loading after TOTP submission
            
            await page.wait_for_timeout(1000)  # Wait for 1 second
            
            # Check for error message after TOTP submission
            error_message = await page.query_selector("text='Invalid TOTP'")
            if error_message:
                logger.error("Invalid TOTP code detected")
                await page.screenshot(path="invalid_totp.png")
                raise Exception("Invalid TOTP code")
            
            # After redirection, take a screenshot for debugging
            await page.screenshot(path="after_totp_submission.png")
            
            # Now the page should have the request token URL
            final_url = page.url
            logger.info(f"Final URL (after TOTP): {final_url}")
            
            # Wait for the page to redirect to localhost:8080
            try:
                # Wait up to 20 seconds for redirection
                for _ in range(20):
                    if "localhost:8080" in page.url or "request_token" in page.url:
                        break
                    await page.wait_for_timeout(1000)
                
                final_url = page.url
                logger.info(f"Final URL (after waiting for redirect): {final_url}")
                
                # Extract request_token from the URL
                request_token = extract_request_token(final_url)
                logger.info(f"Successfully extracted request_token: {request_token}")
                return request_token
            except Exception as e:
                logger.error(f"Error waiting for redirection: {str(e)}")
                # If we still have the URL, try to extract token anyway
                try:
                    request_token = extract_request_token(page.url)
                    logger.info(f"Extracted request_token from current URL: {request_token}")
                    return request_token
                except:
                    raise Exception("Failed to extract request token even from current URL")
            
        except Exception as e:
            logger.error(f"Error during automation: {str(e)}")
            await page.screenshot(path="error_screenshot.png")
            logger.info("Error screenshot saved as error_screenshot.png")
            
        finally:
            await browser.close()

def get_access_token(request_token):
    try:
        kite = KiteConnect(api_key=API_KEY)
        data = kite.generate_session(request_token, api_secret=API_SECRET)
        access_token = data["access_token"] # type:ignore
        logger.info("Successfully generated access token")
        return access_token
    except Exception as e:
        raise Exception(f"Failed to generate access token: {str(e)}")

def update_env(access_token):
    try:
        # Ensure the directory exists
        os.makedirs(os.path.dirname(ENV_PATH), exist_ok=True)
        
        # Create .env if it doesn't exist
        if not os.path.exists(ENV_PATH):
            with open(ENV_PATH, 'w') as f:
                f.write("")
        
        # Update the access token
        load_dotenv(ENV_PATH)
        set_key(ENV_PATH, "ZERODHA_ACCESS_TOKEN", access_token)
        set_key(ENV_PATH, "ZERODHA_API_KEY", API_KEY)
        set_key(ENV_PATH, "ZERODHA_API_SECRET", API_SECRET)
        logger.info(f"Updated access token and API keys in {ENV_PATH}")
        
        # Verify the token was written
        load_dotenv(ENV_PATH)
        saved_token = os.getenv("ZERODHA_ACCESS_TOKEN")
        if saved_token != access_token:
            raise Exception("Token verification failed")
            
    except Exception as e:
        raise Exception(f"Failed to update .env file: {str(e)}")

async def main():
    try:
        logger.info("Starting Zerodha token automation...")
        request_token = await get_request_token()
        logger.info("Got request token, generating access token...")
        access_token = get_access_token(request_token)
        logger.info("Got access token, updating .env file...")
        update_env(access_token)
        logger.info("Access token automation completed successfully!")
        logger.info("You can now run zerodha_run_connector.py")
    except Exception as e:
        logger.error(f"Automation failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main()) 