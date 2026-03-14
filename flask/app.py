from flask import Flask, request, jsonify
import sqlite3
import pandas as pd
import os
from datetime import datetime
from database import DatabaseManager
import json
from io import StringIO, BytesIO

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

# Initialize database manager
db_manager = DatabaseManager()

def allowed_file(filename):
    """Checks if file type is allowed"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in {'csv', 'xlsx'}

def read_file_from_memory(file_content, file_ext, filename):
    """Reads file directly from memory"""
    try:
        if file_ext == 'csv':
            # Try different encodings and separators
            encodings = ['utf-8', 'cp1251', 'windows-1251', 'latin-1']
            separators = [',', ';', '\t']
            
            # Decode content once
            content_str = file_content.decode('utf-8', errors='ignore')
            
            for encoding in encodings:
                for sep in separators:
                    try:
                        df = pd.read_csv(StringIO(content_str), encoding=encoding, sep=sep)
                        if len(df.columns) > 1:  # Successfully read more than one column
                            return df, None
                    except:
                        continue
            return None, "Could not read CSV file. Check encoding and separator."
        
        elif file_ext == 'xlsx':
            df = pd.read_excel(BytesIO(file_content), engine='openpyxl')
            for col in df.columns:
                df[col] = df[col].astype(str)
            return df, None
        
        else:
            return None, "Unsupported file format"
            
    except Exception as e:
        return None, f"Error reading file: {str(e)}"

@app.route('/', methods=['GET'])
def home():
    """Home page with welcome message and API documentation"""
    html_content = '''
    <!DOCTYPE html>
    <html>
    <head>
        <title>Risk Tech Service</title>
        <style>
            body { 
                font-family: 'Segoe UI', Arial, sans-serif; 
                margin: 40px; 
                max-width: 900px;
                margin: 0 auto;
                padding: 20px;
                line-height: 1.6;
                color: #333;
            }
            h1 { 
                color: #2c3e50; 
                border-bottom: 2px solid #3498db;
                padding-bottom: 10px;
            }
            h2 { color: #2c3e50; margin-top: 30px; }
            .endpoint { 
                background: #f8f9fa; 
                padding: 15px; 
                margin: 15px 0; 
                border-left: 4px solid #3498db;
                border-radius: 0 5px 5px 0;
                box-shadow: 0 2px 5px rgba(0,0,0,0.1);
            }
            code { 
                background: #e9ecef; 
                padding: 3px 8px; 
                border-radius: 4px;
                font-family: 'Courier New', monospace;
                color: #c7254e;
            }
            pre {
                background: #272822;
                color: #f8f8f2;
                padding: 15px;
                border-radius: 5px;
                overflow-x: auto;
            }
            .method {
                display: inline-block;
                padding: 3px 10px;
                border-radius: 4px;
                color: white;
                font-weight: bold;
                margin-right: 10px;
            }
            .get { background: #27ae60; }
            .post { background: #f39c12; }
            .footer {
                margin-top: 50px;
                padding-top: 20px;
                border-top: 1px solid #eee;
                text-align: center;
                color: #7f8c8d;
            }
        </style>
    </head>
    <body>
        <h1>Risk Tech Service API</h1>
        <p>Welcome to the Risk Tech API service for uploading CSV/XLSX files to SQLite database.</p>
        
        <h2>Available Endpoints:</h2>
        
        <div class="endpoint">
            <span class="method get">GET</span> <code>/</code>
            <p>This documentation page</p>
        </div>
        
        <div class="endpoint">
            <span class="method get">GET</span> <code>/health</code>
            <p>Health check - verify service is running</p>
            <code>curl http://localhost:5000/health</code>
        </div>
        
        <div class="endpoint">
            <span class="method get">GET</span> <code>/tables</code>
            <p>List all tables in database</p>
            <code>curl http://localhost:5000/tables</code>
        </div>
        
        <div class="endpoint">
            <span class="method get">GET</span> <code>/table/&lt;table_name&gt;</code>
            <p>View contents of specific table</p>
            <code>curl http://localhost:5000/table/table_name</code>
        </div>
        
        <div class="endpoint">
            <span class="method post">POST</span> <code>/upload</code>
            <p>Upload CSV or XLSX file (send file in 'file' field)</p>
            <code>curl -X POST http://localhost:5000/upload -F "file=@filename"</code>
        </div>
        
        <h2>Example Usage:</h2>
        <pre>
# 1. Upload a file
curl -X POST http://localhost:5000/upload -F "file=@filename"

# 2. List all tables
curl http://localhost:5000/tables

# 3. View table data
curl http://localhost:5000/table/table_name

# 4. Check service health
curl http://localhost:5000/health
        </pre>
        
        <div class="footer">
            <p>Risk Tech Service v1.0.0 | Python Flask + SQLite</p>
        </div>
    </body>
    </html>
    '''
    return html_content

@app.route('/upload', methods=['POST'])
def upload_file():
    """
    Endpoint for uploading file and creating database table
    """
    # Check if file exists in request
    if 'file' not in request.files:
        return jsonify({
            'success': False,
            'error': 'No file found in request'
        }), 400
    
    file = request.files['file']
    
    # Check if file is selected
    if file.filename == '':
        return jsonify({
            'success': False,
            'error': 'Filename cannot be empty'
        }), 400
    
    # Check file extension
    if not allowed_file(file.filename):
        return jsonify({
            'success': False,
            'error': 'Only .csv and .xlsx files are allowed'
        }), 400
    
    file_ext = file.filename.rsplit('.', 1)[1].lower()
    
    try:
        # Read file content into memory
        file_content = file.read()
        
        # Check if file is empty
        if len(file_content) == 0:
            return jsonify({
                'success': False,
                'error': 'File is empty'
            }), 400
        
        # Read file from memory
        df, error = read_file_from_memory(file_content, file_ext, file.filename)
        if error:
            return jsonify({
                'success': False,
                'error': error
            }), 400
        
        # Check if DataFrame is empty (no data rows)
        if df.empty:
            return jsonify({
                'success': False,
                'error': 'File contains no data'
            }), 400
        
        # Generate sequential table name
        existing_tables = db_manager.get_table_names()
        next_number = len(existing_tables) + 1
        table_name = f"table_{next_number}"
        
        # Create table in database
        success, message, column_types = db_manager.create_table_from_dataframe(df, table_name)
        
        if success:
            # Get information about created table
            table_info = {
                'table_name': table_name,
                'columns': list(df.columns),
                'rows': len(df),
                'data_types': {col: column_types[i] for i, col in enumerate(df.columns)}
            }
            
            return jsonify({
                'success': True,
                'message': message,
                'table_info': table_info
            }), 200
        else:
            return jsonify({
                'success': False,
                'error': message
            }), 500
            
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Internal server error: {str(e)}'
        }), 500

@app.route('/tables', methods=['GET'])
def list_tables():
    """Returns list of all tables in database"""
    tables = db_manager.get_table_names()
    return jsonify({
        'success': True,
        'tables': tables
    })

@app.route('/table/<table_name>', methods=['GET'])
def view_table(table_name):
    """Returns contents of specified table with proper Unicode support"""
    df = db_manager.get_table_data(table_name)
    if df is not None:
        conn = sqlite3.connect(db_manager.db_path)
        cursor = conn.cursor()
        cursor.execute(f'PRAGMA table_info("{table_name}")')
        columns_info = cursor.fetchall()
        conn.close()
        
        # Dictionary {col_name: type}
        column_types = {}
        for col in columns_info:
            col_name = col[1]
            col_type = col[2]
            column_types[col_name] = col_type
        
        return app.response_class(
            response=json.dumps({
                'success': True,
                'table_name': table_name,
                'columns': list(df.columns),
                'data_types': column_types,
                'data': df.to_dict(orient='records')
            }, ensure_ascii=False, indent=2),
            status=200,
            mimetype='application/json'
        )
    else:
        return jsonify({
            'success': False,
            'error': f'Table "{table_name}" not found'
        }), 404

@app.route('/health', methods=['GET'])
def health_check():
    """Service health check"""
    return jsonify({
        'success': True,
        'status': 'running',
        'timestamp': datetime.now().isoformat()
    })

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)