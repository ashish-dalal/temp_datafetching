import os
import sys

def create_env_file():
    """Create a proper .env file with all necessary configuration"""
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    
    print(f"Creating/updating .env file at: {env_path}")
    
    # Get existing values if the file exists
    existing_values = {}
    if os.path.exists(env_path):
        print("Found existing .env file, preserving current values.")
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    existing_values[key.strip()] = value.strip()
    
    # Define required parameters
    required_params = [
        'ZERODHA_API_KEY',
        'ZERODHA_API_SECRET',
        'ZERODHA_ACCESS_TOKEN',
        'QUESTDB_HOST',
        'QUESTDB_PORT',
        'QUESTDB_USER',
        'QUESTDB_PASSWORD'
    ]
    
    # Check which parameters are missing
    missing_params = [param for param in required_params if param not in existing_values]
    
    # If any parameters are missing, prompt for input
    if missing_params:
        print(f"\nThe following parameters are missing: {', '.join(missing_params)}")
        print("Please provide these values:\n")
        
        for param in missing_params:
            # Set default values based on parameter name
            default_value = ""
            if param == 'ZERODHA_API_KEY':
                default_value = "n3a1ly52x1d99ntm"
            elif param == 'ZERODHA_API_SECRET':
                default_value = "kdatn7jgpehykadplml2f91hxhsuk7ht"
            elif param == 'QUESTDB_HOST':
                default_value = "qdb3.satyvm.com"
            elif param == 'QUESTDB_PORT':
                default_value = "443"
            elif param == 'QUESTDB_USER':
                default_value = "2Cents"
            elif param == 'QUESTDB_PASSWORD':
                default_value = "2Cents$1012cc"
                
            # Prompt for input with default value
            if default_value:
                prompt = f"{param} [default: {default_value}]: "
            else:
                prompt = f"{param}: "
                
            # Get user input
            value = input(prompt)
            
            # Use default if no input provided
            if not value and default_value:
                value = default_value
                print(f"Using default value for {param}: {default_value}")
                
            existing_values[param] = value
    
    # Create the .env file
    with open(env_path, 'w') as f:
        for param in required_params:
            if param in existing_values:
                f.write(f"{param}={existing_values[param]}\n")
            else:
                print(f"Warning: {param} is still missing!")
    
    print(f"\n.env file created/updated successfully at {env_path}")
    print("Contents (with sensitive values masked):")
    for param in required_params:
        value = existing_values.get(param, "MISSING")
        # Mask sensitive values
        if "SECRET" in param or "PASSWORD" in param or "TOKEN" in param:
            masked_value = value[:4] + "..." + value[-4:] if len(value) > 8 else "****"
        else:
            masked_value = value
        print(f"  {param}={masked_value}")
    
    return True

if __name__ == "__main__":
    print("Zerodha Data Fetcher - Environment Setup")
    print("========================================\n")
    
    success = create_env_file()
    
    if success:
        print("\nSetup completed successfully.")
        print("\nNext steps:")
        print("1. If you need to generate a new Zerodha access token, run:")
        print("   python listner.py")
        print("   python zerodha_token_automation.py")
        print("2. To test the QuestDB connection, run:")
        print("   python check_questdb_http.py")
        print("3. To start the data fetcher, run:")
        print("   python zerodha_run_connector.py")
    else:
        print("\nSetup failed. Please check the errors above.")
    
    sys.exit(0 if success else 1) 