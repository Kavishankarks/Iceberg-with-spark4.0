import streamlit as st
import json
import os
import pandas as pd
import pyarrow as pa
from pathlib import Path
from datetime import datetime
import glob
import fastavro
import io

def flatten_dict(d, parent_key='', sep='_'):
    """
    Flatten a nested dictionary, converting lists and other complex types to strings
    """
    items = []
    if isinstance(d, dict):
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                # Convert lists to string representation
                items.append((new_key, str(v)))
            elif isinstance(v, (bytes, bytearray)):
                # Convert bytes to string
                try:
                    items.append((new_key, v.decode('utf-8')))
                except UnicodeDecodeError:
                    items.append((new_key, str(v)))
            else:
                items.append((new_key, v))
    elif isinstance(d, list):
        return str(d)
    else:
        return d
    return dict(items)

# Page configuration
st.set_page_config(
    page_title="Iceberg Metadata & Avro Viewer",
    page_icon="🧊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Title
st.title("🧊 Iceberg Metadata & Avro File Viewer")
st.markdown("---")

# Sidebar for navigation
st.sidebar.title("Navigation")
page = st.sidebar.selectbox(
    "Choose a page:",
    ["📋 Metadata Browser", "📊 Avro File Viewer", "📈 Table Statistics"]
)

# Default warehouse path
default_warehouse = "/Users/kavishankarks/Documents/GitHub/spark-iceberg-overview/warehouse/demo"

if page == "📋 Metadata Browser":
    st.header("📋 Iceberg Metadata Browser")
    
    # Warehouse path input
    warehouse_path = st.text_input(
        "Warehouse Path:",
        value=default_warehouse,
        help="Path to your Iceberg warehouse directory"
    )
    
    if os.path.exists(warehouse_path):
        # Find all tables
        tables = []
        for root, dirs, files in os.walk(warehouse_path):
            if "metadata" in dirs:
                table_path = root
                table_name = table_path.replace(warehouse_path, "").strip("/")
                if table_name:
                    tables.append((table_name, table_path))
        
        if tables:
            # Table selection
            table_options = [f"{name} ({path})" for name, path in tables]
            selected_table = st.selectbox("Select Table:", table_options)
            
            if selected_table:
                # Extract table path
                table_name = selected_table.split(" (")[0]
                table_path = [path for name, path in tables if name == table_name][0]
                metadata_path = os.path.join(table_path, "metadata")
                
                # Display metadata files
                st.subheader(f"📁 Metadata Files for {table_name}")
                
                if os.path.exists(metadata_path):
                    metadata_files = [f for f in os.listdir(metadata_path) if f.endswith('.metadata.json')]
                    metadata_files.sort(reverse=True)  # Latest first
                    
                    if metadata_files:
                        # File selection
                        selected_file = st.selectbox("Select Metadata File:", metadata_files)
                        
                        if selected_file:
                            file_path = os.path.join(metadata_path, selected_file)
                            
                            # Display file info
                            file_stats = os.stat(file_path)
                            col1, col2, col3 = st.columns(3)
                            
                            with col1:
                                st.metric("File Size", f"{file_stats.st_size} bytes")
                            with col2:
                                st.metric("Last Modified", datetime.fromtimestamp(file_stats.st_mtime).strftime("%Y-%m-%d %H:%M:%S"))
                            with col3:
                                st.metric("Metadata Version", selected_file.split('.')[0])
                            
                            # Load and display metadata
                            try:
                                with open(file_path, 'r') as f:
                                    metadata = json.load(f)
                                
                                # Tabs for different views
                                tab1, tab2, tab3, tab4, tab5 = st.tabs(["📊 Overview", "🗂️ Schema", "📸 Snapshots", "📁 Files", "🔧 Raw JSON"])
                                
                                with tab1:
                                    st.subheader("Table Overview")
                                    col1, col2 = st.columns(2)
                                    
                                    with col1:
                                        st.write(f"**Format Version:** {metadata.get('format-version', 'N/A')}")
                                        st.write(f"**Table UUID:** {metadata.get('table-uuid', 'N/A')}")
                                        st.write(f"**Location:** {metadata.get('location', 'N/A')}")
                                        
                                    with col2:
                                        last_updated = metadata.get('last-updated-ms', 0)
                                        if last_updated:
                                            last_updated_str = datetime.fromtimestamp(last_updated/1000).strftime("%Y-%m-%d %H:%M:%S")
                                            st.write(f"**Last Updated:** {last_updated_str}")
                                        
                                        current_snapshot = metadata.get('current-snapshot-id')
                                        if current_snapshot:
                                            st.write(f"**Current Snapshot ID:** {current_snapshot}")
                                    
                                    # Table properties
                                    properties = metadata.get('properties', {})
                                    if properties:
                                        st.subheader("Table Properties")
                                        props_df = pd.DataFrame(list(properties.items()), columns=['Property', 'Value'])
                                        st.dataframe(props_df, use_container_width=True)
                                
                                with tab2:
                                    st.subheader("Table Schema")
                                    schemas = metadata.get('schemas', [])
                                    current_schema_id = metadata.get('current-schema-id', 0)
                                    
                                    for schema in schemas:
                                        if schema.get('schema-id') == current_schema_id:
                                            fields = schema.get('fields', [])
                                            
                                            schema_data = []
                                            for field in fields:
                                                schema_data.append({
                                                    'Field ID': field.get('id', ''),
                                                    'Name': field.get('name', ''),
                                                    'Type': field.get('type', ''),
                                                    'Required': 'Yes' if field.get('required', False) else 'No'
                                                })
                                            
                                            if schema_data:
                                                schema_df = pd.DataFrame(schema_data)
                                                st.dataframe(schema_df, use_container_width=True)
                                            break
                                
                                with tab3:
                                    st.subheader("Snapshots")
                                    snapshots = metadata.get('snapshots', [])
                                    
                                    if snapshots:
                                        snapshot_data = []
                                        for snapshot in snapshots:
                                            timestamp = snapshot.get('timestamp-ms', 0)
                                            timestamp_str = datetime.fromtimestamp(timestamp/1000).strftime("%Y-%m-%d %H:%M:%S") if timestamp else 'N/A'
                                            
                                            summary = snapshot.get('summary', {})
                                            operation = summary.get('operation', 'unknown')
                                            added_records = summary.get('added-records', '0')
                                            
                                            snapshot_data.append({
                                                'Snapshot ID': snapshot.get('snapshot-id', ''),
                                                'Timestamp': timestamp_str,
                                                'Operation': operation,
                                                'Added Records': added_records,
                                                'Schema ID': snapshot.get('schema-id', '')
                                            })
                                        
                                        snapshots_df = pd.DataFrame(snapshot_data)
                                        st.dataframe(snapshots_df, use_container_width=True)
                                    else:
                                        st.info("No snapshots found")
                                
                                with tab4:
                                    st.subheader("Manifest Files")
                                    manifests = metadata.get('manifests', [])
                                    
                                    if manifests:
                                        manifest_data = []
                                        for manifest in manifests:
                                            manifest_path = manifest.get('manifest-path', '')
                                            filename = os.path.basename(manifest_path) if manifest_path else 'N/A'
                                            
                                            manifest_data.append({
                                                'Manifest File': filename,
                                                'Length': manifest.get('manifest-length', 0),
                                                'Added Files': manifest.get('added-files-count', 0),
                                                'Existing Files': manifest.get('existing-files-count', 0),
                                                'Deleted Files': manifest.get('deleted-files-count', 0)
                                            })
                                        
                                        manifests_df = pd.DataFrame(manifest_data)
                                        st.dataframe(manifests_df, use_container_width=True)
                                    else:
                                        st.info("No manifest files found")
                                
                                with tab5:
                                    st.subheader("Raw JSON Metadata")
                                    st.json(metadata)
                                    
                                    # Download button
                                    st.download_button(
                                        label="Download JSON",
                                        data=json.dumps(metadata, indent=2),
                                        file_name=selected_file,
                                        mime="application/json"
                                    )
                                
                            except Exception as e:
                                st.error(f"Error reading metadata file: {str(e)}")
                    else:
                        st.warning("No metadata files found in the selected table")
                else:
                    st.error("Metadata directory not found")
        else:
            st.warning("No Iceberg tables found in the specified warehouse path")
    else:
        st.error("Warehouse path does not exist")

elif page == "📊 Avro File Viewer":
    st.header("📊 Avro File Viewer")
    
    # File path input
    avro_path = st.text_input(
        "Avro File or Directory Path:",
        value=default_warehouse,
        help="Path to Avro file or directory containing Avro files"
    )
    
    if os.path.exists(avro_path):
        avro_files = []
        
        if os.path.isfile(avro_path) and avro_path.endswith('.avro'):
            avro_files = [avro_path]
        elif os.path.isdir(avro_path):
            # Find all Avro files recursively
            for root, dirs, files in os.walk(avro_path):
                for file in files:
                    if file.endswith('.avro'):
                        avro_files.append(os.path.join(root, file))
        
        if avro_files:
            # File selection
            selected_avro = st.selectbox("Select Avro File:", avro_files)
            
            if selected_avro:
                try:
                    # Read Avro file
                    records = []
                    avro_schema = None
                    
                    with open(selected_avro, 'rb') as f:
                        reader = fastavro.reader(f)
                        avro_schema = reader.metadata.get('avro.schema')
                        
                        # Read all records
                        for record in reader:
                            records.append(record)
                    
                    if records:
                        # Flatten complex nested structures for pandas compatibility
                        flattened_records = []
                        for record in records:
                            flattened_record = flatten_dict(record)
                            flattened_records.append(flattened_record)
                        
                        df = pd.DataFrame(flattened_records)
                        
                        # File info
                        file_stats = os.stat(selected_avro)
                        col1, col2, col3, col4 = st.columns(4)
                        
                        with col1:
                            st.metric("File Size", f"{file_stats.st_size:,} bytes")
                        with col2:
                            st.metric("Rows", f"{len(df):,}")
                        with col3:
                            st.metric("Columns", len(df.columns))
                        with col4:
                            st.metric("Memory Usage", f"{df.memory_usage(deep=True).sum():,} bytes")
                        
                        # Tabs for different views
                        tab1, tab2, tab3, tab4, tab5 = st.tabs(["📊 Data Preview", "📋 Schema Info", "🔧 Avro Schema", "📈 Statistics", "🔍 Query"])
                        
                        with tab1:
                            st.subheader("Data Preview")
                            
                            # Show options
                            col1, col2 = st.columns(2)
                            with col1:
                                show_rows = st.slider("Number of rows to display:", 5, min(1000, len(df)), 10)
                            with col2:
                                show_all_cols = st.checkbox("Show all columns", value=True)
                            
                            if show_all_cols:
                                st.dataframe(df.head(show_rows), use_container_width=True)
                            else:
                                selected_cols = st.multiselect("Select columns:", df.columns.tolist(), default=df.columns.tolist()[:5])
                                if selected_cols:
                                    st.dataframe(df[selected_cols].head(show_rows), use_container_width=True)
                        
                        with tab2:
                            st.subheader("Schema Information")
                            
                            schema_info = []
                            for i, (col_name, dtype) in enumerate(zip(df.columns, df.dtypes)):
                                null_count = df[col_name].isnull().sum()
                                unique_count = df[col_name].nunique()
                                
                                schema_info.append({
                                    'Column': col_name,
                                    'Data Type': str(dtype),
                                    'Null Count': null_count,
                                    'Null %': f"{(null_count/len(df)*100):.1f}%",
                                    'Unique Values': unique_count
                                })
                            
                            schema_df = pd.DataFrame(schema_info)
                            st.dataframe(schema_df, use_container_width=True)
                        
                        with tab3:
                            st.subheader("Avro Schema")
                            if avro_schema:
                                try:
                                    schema_dict = json.loads(avro_schema)
                                    st.json(schema_dict)
                                    
                                    # Show field details if it's a record
                                    if schema_dict.get('type') == 'record' and 'fields' in schema_dict:
                                        st.subheader("Field Details")
                                        field_data = []
                                        for field in schema_dict['fields']:
                                            field_type = field.get('type', 'unknown')
                                            if isinstance(field_type, list):
                                                # Union type
                                                field_type = f"Union: {', '.join([str(t) for t in field_type])}"
                                            elif isinstance(field_type, dict):
                                                # Complex type
                                                field_type = f"{field_type.get('type', 'complex')}"
                                            
                                            field_data.append({
                                                'Field Name': field.get('name', ''),
                                                'Type': str(field_type),
                                                'Default': field.get('default', 'None'),
                                                'Doc': field.get('doc', '')
                                            })
                                        
                                        if field_data:
                                            fields_df = pd.DataFrame(field_data)
                                            st.dataframe(fields_df, use_container_width=True)
                                
                                except json.JSONDecodeError:
                                    st.text(avro_schema)
                            else:
                                st.info("No Avro schema metadata found")
                        
                        with tab4:
                            st.subheader("Data Statistics")
                            
                            # Numeric columns only
                            numeric_df = df.select_dtypes(include=['number'])
                            if not numeric_df.empty:
                                st.write("**Numeric Columns:**")
                                st.dataframe(numeric_df.describe(), use_container_width=True)
                            
                            # String columns
                            string_df = df.select_dtypes(include=['object'])
                            if not string_df.empty:
                                st.write("**String Columns (Top 5 values):**")
                                for col in string_df.columns[:3]:  # Show top 3 string columns
                                    st.write(f"**{col}:**")
                                    value_counts = df[col].value_counts().head()
                                    st.write(value_counts.to_dict())
                        
                        with tab5:
                            st.subheader("Simple Query Interface")
                            
                            # Column filter
                            filter_col = st.selectbox("Filter by column:", ["None"] + df.columns.tolist())
                            
                            if filter_col != "None":
                                if df[filter_col].dtype in ['object']:
                                    # String column
                                    unique_vals = df[filter_col].unique()[:50]  # Limit to 50 values
                                    filter_val = st.selectbox(f"Filter {filter_col} equals:", ["All"] + list(unique_vals))
                                    
                                    if filter_val != "All":
                                        filtered_df = df[df[filter_col] == filter_val]
                                    else:
                                        filtered_df = df
                                else:
                                    # Numeric column
                                    min_val, max_val = float(df[filter_col].min()), float(df[filter_col].max())
                                    range_vals = st.slider(f"Filter {filter_col} range:", min_val, max_val, (min_val, max_val))
                                    filtered_df = df[(df[filter_col] >= range_vals[0]) & (df[filter_col] <= range_vals[1])]
                            else:
                                filtered_df = df
                            
                            st.write(f"**Filtered Results ({len(filtered_df)} rows):**")
                            st.dataframe(filtered_df.head(100), use_container_width=True)
                            
                            # Download filtered data
                            csv = filtered_df.to_csv(index=False)
                            st.download_button(
                                label="Download filtered data as CSV",
                                data=csv,
                                file_name="filtered_data.csv",
                                mime="text/csv"
                            )
                    else:
                        st.warning("No records found in the Avro file")
                
                except Exception as e:
                    st.error(f"Error reading Avro file: {str(e)}")
                    st.error("Please ensure the file is a valid Avro format")
        else:
            st.warning("No Avro files found in the specified path")
    else:
        st.error("Path does not exist")

elif page == "📈 Table Statistics":
    st.header("📈 Table Statistics & Summary")
    
    warehouse_path = st.text_input(
        "Warehouse Path:",
        value=default_warehouse,
        help="Path to your Iceberg warehouse directory"
    )
    
    if os.path.exists(warehouse_path):
        # Collect statistics from all tables
        tables_stats = []
        
        for root, dirs, files in os.walk(warehouse_path):
            if "metadata" in dirs:
                table_path = root
                table_name = table_path.replace(warehouse_path, "").strip("/")
                if table_name:
                    metadata_path = os.path.join(table_path, "metadata")
                    
                    # Find latest metadata file
                    metadata_files = [f for f in os.listdir(metadata_path) if f.endswith('.metadata.json')]
                    if metadata_files:
                        latest_file = max(metadata_files, key=lambda x: os.path.getmtime(os.path.join(metadata_path, x)))
                        
                        try:
                            with open(os.path.join(metadata_path, latest_file), 'r') as f:
                                metadata = json.load(f)
                            
                            # Extract statistics
                            snapshots = metadata.get('snapshots', [])
                            schemas = metadata.get('schemas', [])
                            current_schema_id = metadata.get('current-schema-id', 0)
                            
                            # Get current schema
                            current_schema = None
                            for schema in schemas:
                                if schema.get('schema-id') == current_schema_id:
                                    current_schema = schema
                                    break
                            
                            num_columns = len(current_schema.get('fields', [])) if current_schema else 0
                            num_snapshots = len(snapshots)
                            
                            # Latest snapshot info
                            latest_snapshot = snapshots[-1] if snapshots else {}
                            last_operation = latest_snapshot.get('summary', {}).get('operation', 'N/A')
                            
                            tables_stats.append({
                                'Table': table_name,
                                'Columns': num_columns,
                                'Snapshots': num_snapshots,
                                'Last Operation': last_operation,
                                'Format Version': metadata.get('format-version', 'N/A'),
                                'Metadata File': latest_file
                            })
                        
                        except Exception as e:
                            tables_stats.append({
                                'Table': table_name,
                                'Columns': 'Error',
                                'Snapshots': 'Error',
                                'Last Operation': 'Error',
                                'Format Version': 'Error',
                                'Metadata File': f"Error: {str(e)}"
                            })
        
        if tables_stats:
            st.subheader("Tables Overview")
            stats_df = pd.DataFrame(tables_stats)
            st.dataframe(stats_df, use_container_width=True)
            
            # Summary metrics
            st.subheader("Warehouse Summary")
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Tables", len(tables_stats))
            with col2:
                total_columns = sum([int(row['Columns']) for row in tables_stats if str(row['Columns']).isdigit()])
                st.metric("Total Columns", total_columns)
            with col3:
                total_snapshots = sum([int(row['Snapshots']) for row in tables_stats if str(row['Snapshots']).isdigit()])
                st.metric("Total Snapshots", total_snapshots)
            with col4:
                format_versions = [row['Format Version'] for row in tables_stats if row['Format Version'] != 'Error']
                latest_format = max(format_versions) if format_versions else 'N/A'
                st.metric("Latest Format Version", latest_format)
        else:
            st.warning("No tables found in the warehouse")
    else:
        st.error("Warehouse path does not exist")

# Footer
st.markdown("---")
st.markdown("*🧊 Iceberg Metadata & Avro Viewer - Built with Streamlit*")