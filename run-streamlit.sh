#!/bin/bash

# Script to run the Streamlit Iceberg Metadata & ORC Viewer

echo "=== Iceberg Metadata & ORC Viewer (Streamlit) ==="

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 is required but not installed."
    exit 1
fi

# Check if virtual environment exists
if [ ! -d "streamlit-env" ]; then
    echo "Creating Python virtual environment..."
    python3 -m venv streamlit-env
fi

# Activate virtual environment
echo "Activating virtual environment..."
source streamlit-env/bin/activate

# Install/upgrade requirements
echo "Installing Python dependencies..."
pip install --upgrade pip -q
pip install -r streamlit-app/requirements.txt -q

if [ $? -eq 0 ]; then
    echo "Dependencies installed successfully."
    echo ""
    echo "Starting Streamlit app..."
    echo "The app will open in your default browser at http://localhost:8501"
    echo ""
    echo "To stop the app, press Ctrl+C"
    echo ""
    
    # Change to streamlit-app directory and run the app
    cd streamlit-app
    streamlit run app.py
else
    echo "Error: Failed to install dependencies."
    exit 1
fi