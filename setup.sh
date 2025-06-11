#!/bin/bash

# Server Setup Script for Data pipeline
# This script installs Docker/Docker Compose on a new server, clones the repo, then runs run.sh

set -e  # Exit on any error

echo "Server Setup for Data pipeline Starting..."
echo "============================================="

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
GITHUB_TOKEN=""
GITHUB_REPO="https://github.com/2CentsCapital/datafetching.git"
PROJECT_DIR="datafetching"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --github-token)
            GITHUB_TOKEN="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [--github-token TOKEN]"
            echo ""
            echo "Options:"
            echo "  --github-token TOKEN    GitHub personal access token for repository access"
            echo "  -h, --help             Show this help message"
            echo ""
            echo "Example:"
            echo "  $0 --github-token ghp_xxxxxxxxxxxxxxxxxxxx"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use -h or --help for usage information"
            exit 1
            ;;
    esac
done

# Logging function
log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}"
}

warn() {
    echo -e "${YELLOW}[WARNING] $1${NC}"
}

error() {
    echo -e "${RED}[ERROR] $1${NC}"
    exit 1
}

info() {
    echo -e "${BLUE}[INFO] $1${NC}"
}

# Check if running as root (for Docker installation)
check_sudo() {
    if [ "$EUID" -eq 0 ]; then
        warn "Running as root. This is fine for Docker installation."
    else
        info "Running as non-root user. Will use sudo for Docker installation."
        # Check if sudo is available
        if ! command -v sudo &> /dev/null; then
            error "sudo is not available. Please run as root or install sudo."
        fi
    fi
}

# Detect OS
detect_os() {
    if [ -f /etc/os-release ]; then
        . /etc/os-release
        OS=$NAME
        VERSION=$VERSION_ID
    else
        error "Cannot detect OS. This script supports Ubuntu/Debian/CentOS/RHEL."
    fi
    
    log "Detected OS: $OS $VERSION"
}

# Install Docker
install_docker() {
    log "Checking Docker installation..."
    
    if command -v docker &> /dev/null; then
        log "Docker is already installed: $(docker --version)"
        return 0
    fi
    
    log "Installing Docker..."
    
    case "$OS" in
        *"Ubuntu"*|*"Debian"*)
            # Update package index
            sudo apt-get update
            
            # Install prerequisites
            sudo apt-get install -y \
                apt-transport-https \
                ca-certificates \
                curl \
                gnupg \
                lsb-release
            
            # Add Docker's official GPG key
            curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
            
            # Set up stable repository
            echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
            
            # Install Docker Engine
            sudo apt-get update
            sudo apt-get install -y docker-ce docker-ce-cli containerd.io
            ;;
            
        *"CentOS"*|*"Red Hat"*|*"RHEL"*)
            # Remove old versions
            sudo yum remove -y docker docker-client docker-client-latest docker-common docker-latest docker-latest-logrotate docker-logrotate docker-engine
            
            # Install yum-utils
            sudo yum install -y yum-utils
            
            # Set up stable repository
            sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
            
            # Install Docker Engine
            sudo yum install -y docker-ce docker-ce-cli containerd.io
            ;;
            
        *)
            error "Unsupported OS: $OS. Please install Docker manually."
            ;;
    esac
    
    # Start Docker service
    sudo systemctl start docker
    sudo systemctl enable docker
    
    # Add current user to docker group (if not root)
    if [ "$EUID" -ne 0 ]; then
        sudo usermod -aG docker $USER
        warn "Added $USER to docker group. You may need to log out and back in for this to take effect."
        warn "Or run: newgrp docker"
    fi
    
    log "Docker installed successfully: $(docker --version)"
}

# Install Docker Compose
install_docker_compose() {
    log "Checking Docker Compose installation..."
    
    if command -v docker-compose &> /dev/null; then
        log "Docker Compose is already installed: $(docker-compose --version)"
        return 0
    fi
    
    log "Installing Docker Compose..."
    
    # Download Docker Compose
    DOCKER_COMPOSE_VERSION="2.24.0"
    sudo curl -L "https://github.com/docker/compose/releases/download/v${DOCKER_COMPOSE_VERSION}/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
    
    # Make it executable
    sudo chmod +x /usr/local/bin/docker-compose
    
    # Create symlink for easy access
    sudo ln -sf /usr/local/bin/docker-compose /usr/bin/docker-compose
    
    log "Docker Compose installed successfully: $(docker-compose --version)"
}

# Install required system packages
install_system_packages() {
    log "Installing required system packages..."
    
    case "$OS" in
        *"Ubuntu"*|*"Debian"*)
            sudo apt-get update
            sudo apt-get install -y curl wget git unzip
            ;;
        *"CentOS"*|*"Red Hat"*|*"RHEL"*)
            sudo yum install -y curl wget git unzip
            ;;
    esac
    
    log "System packages installed successfully"
}

# Get GitHub credentials
get_github_credentials() {
    log "Setting up GitHub access..."
    
    if [ -n "$GITHUB_TOKEN" ]; then
        log "Using GitHub token provided via --github-token flag"
        return 0
    fi
    
    # Prompt for GitHub token
    echo ""
    info "GitHub Personal Access Token required for repository access"
    info "You can create one at: https://github.com/settings/tokens"
    info "Required permissions: repo (for private repos) or public_repo (for public repos)"
    echo ""
    
    read -p "Enter your GitHub username: " GITHUB_USERNAME
    read -s -p "Enter your GitHub personal access token: " GITHUB_TOKEN
    echo ""
    
    if [ -z "$GITHUB_USERNAME" ] || [ -z "$GITHUB_TOKEN" ]; then
        error "GitHub username and token are required"
    fi
    
    log "GitHub credentials configured"
}

# Clone repository
clone_repository() {
    log "Cloning Data pipeline repository..."
    
    # Remove existing directory if it exists
    if [ -d "$PROJECT_DIR" ]; then
        warn "Directory $PROJECT_DIR already exists. Removing..."
        rm -rf "$PROJECT_DIR"
    fi
    
    # Clone with token authentication
    if [ -n "$GITHUB_USERNAME" ]; then
        # Using username:token format
        CLONE_URL="https://${GITHUB_USERNAME}:${GITHUB_TOKEN}@github.com/2CentsCapital/datafetching.git"
    else
        # Using token only (works for personal access tokens)
        CLONE_URL="https://${GITHUB_TOKEN}@github.com/2CentsCapital/datafetching.git"
    fi
    
    if git clone "$CLONE_URL" "$PROJECT_DIR"; then
        log "Repository cloned successfully"
    else
        error "Failed to clone repository. Please check your credentials and try again."
    fi
    
    # Change to project directory
    cd "$PROJECT_DIR"
    log "Changed to project directory: $(pwd)"
}

# Verify project structure
verify_project_structure() {
    log "Verifying project structure..."
    
    # Check for required directories and files
    required_dirs=("binance" "zerodha")
    required_files=("docker-compose.yml" "run.sh")
    
    for dir in "${required_dirs[@]}"; do
        if [ ! -d "$dir" ]; then
            error "Required directory not found: $dir"
        fi
    done
    
    for file in "${required_files[@]}"; do
        if [ ! -f "$file" ]; then
            error "Required file not found: $file"
        fi
    done
    
    log "Project structure verified successfully"
}

# Test Docker installation
test_docker() {
    log "Testing Docker installation..."
    
    # Test Docker
    if ! docker run --rm hello-world &>/dev/null; then
        error "Docker test failed. Please check Docker installation."
    fi
    
    # Test Docker Compose
    if ! docker-compose version &>/dev/null; then
        error "Docker Compose test failed. Please check installation."
    fi
    
    log "Docker installation test passed"
}

# Check if run.sh exists
check_run_script() {
    if [ ! -f "run.sh" ]; then
        error "run.sh not found in project directory. Please ensure the repository structure is correct."
    fi
    
    log "Found run.sh"
}

# Setup .env file
setup_env_file() {
    log "Setting up .env file..."
    
    if [ -f ".env" ]; then
        warn ".env file already exists. Skipping creation."
        return 0
    fi
    
    info "Creating .env template file..."
    cat > .env << 'EOF'
# Zerodha API Configuration
ZERODHA_API_KEY=your_zerodha_api_key
ZERODHA_API_SECRET=your_zerodha_api_secret
ZERODHA_ACCESS_TOKEN=your_zerodha_access_token

# QuestDB Configuration  
QUESTDB_HOST=localhost
QUESTDB_PORT=9009
QUESTDB_USER=admin
QUESTDB_PASSWORD=quest
EOF
    
    warn "IMPORTANT: Please edit .env file with your actual credentials before running the system!"
    warn "File location: $(pwd)/.env"
    
    log ".env template created successfully"
}

# Run run.sh
run_data_pipeline() {
    log "Docker/Docker Compose setup completed. Now running run.sh..."
    echo ""
    echo "================================================"
    echo "Delegating to run.sh for Data pipeline..."
    echo "================================================"
    
    # Make run.sh executable
    chmod +x run.sh
    
    # Check if .env has been configured
    if grep -q "your_zerodha_api_key" .env 2>/dev/null; then
        warn "NOTICE: .env file contains template values."
        warn "Please edit .env with your actual credentials and then run: ./run.sh"
        info "Skipping automatic run.sh execution."
        return 0
    fi
    
    # Run run.sh
    ./run.sh
    
    if [ $? -eq 0 ]; then
        log "run.sh completed successfully!"
    else
        error "run.sh failed. Please check the output above."
    fi
}

# Display completion message
display_completion() {
    echo ""
    echo "Server Setup Complete!"
    echo "========================="
    echo ""
    echo "Docker: $(docker --version)"
    echo "Docker Compose: $(docker-compose --version)"
    echo "Repository: Cloned to $(pwd)"
    
    if grep -q "your_zerodha_api_key" .env 2>/dev/null; then
        echo "Data pipeline: Ready for configuration"
        echo ""
        echo "Next Steps:"
        echo "1. Edit .env file with your actual credentials:"
        echo "   nano .env"
        echo "2. Start the Data pipeline:"
        echo "   ./run.sh"
    else
        echo "Data pipeline: Started successfully"
    fi
    
    echo ""
    echo "This server is now ready!"
    echo ""
    echo "Management Commands:"
    echo "  View logs:           docker-compose logs -f"
    echo "  Stop system:         docker-compose down"
    echo "  Restart system:      docker-compose restart"
    echo "  Check status:        docker-compose ps"
    echo ""
    echo "Project location: $(pwd)"
    echo ""
}

# Main execution flow
main() {
    log "Starting server setup for Data pipeline..."
    
    # Pre-checks
    check_sudo
    detect_os
    
    # System setup
    install_system_packages
    install_docker
    install_docker_compose
    
    # Test installations
    test_docker
    
    # GitHub and project setup
    get_github_credentials
    clone_repository
    verify_project_structure
    setup_env_file
    check_run_script
    
    # Run the Data pipeline setup
    run_data_pipeline
    
    # Display completion info
    display_completion
    
    log "Server setup completed successfully!"
}

# Handle script interruption
trap 'echo -e "\n${RED}Setup interrupted by user${NC}"; exit 1' INT

# Run main function
main "$@"