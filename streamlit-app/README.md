# 🧊 Iceberg Metadata & Avro Viewer

A Streamlit web application for browsing Iceberg table metadata and viewing Avro files.

## Features

### 📋 Metadata Browser
- Browse Iceberg warehouse tables
- View metadata files with detailed information
- Explore table schemas with field details
- Examine snapshot history and operations
- View manifest files and data organization
- Download metadata as JSON

### 📊 Avro File Viewer
- Load and preview Avro files with fastavro
- View schema information and statistics
- Display native Avro schema with field details
- Support for complex types (unions, records, arrays)
- Filter and query data interactively
- Export filtered data as CSV
- Memory usage and file size metrics

### 📈 Table Statistics
- Warehouse-wide overview
- Tables summary with column counts
- Snapshot statistics
- Format version tracking

## Quick Start

### 1. Run the Application
```bash
./run-streamlit.sh
```

### 2. Access the Web Interface
Open your browser to: `http://localhost:8501`

### 3. Navigate the Interface
- **Metadata Browser**: Explore your Iceberg tables
- **Avro File Viewer**: Analyze Avro data files
- **Table Statistics**: View warehouse summary

## Manual Setup

If you prefer to set up manually:

```bash
# Create virtual environment
python3 -m venv streamlit-env
source streamlit-env/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run the app
cd streamlit-app
streamlit run app.py
```

## Dependencies

- streamlit>=1.28.0
- pandas>=2.0.0
- pyarrow>=12.0.0
- numpy>=1.24.0

## Usage Tips

1. **Default Warehouse Path**: The app defaults to `./warehouse` relative to your project directory
2. **File Selection**: Use the dropdowns to select tables and specific metadata files
3. **Data Preview**: Adjust row limits and column selection in the ORC viewer
4. **Download**: Export metadata JSON or filtered ORC data as CSV
5. **Performance**: Large ORC files are limited to preview mode for performance

## Troubleshooting

- **No tables found**: Ensure your warehouse path contains Iceberg tables with metadata directories
- **ORC files not loading**: Check that PyArrow can read your ORC file format
- **Permission errors**: Ensure the app has read access to your warehouse directory