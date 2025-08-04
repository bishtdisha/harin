from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import pandas as pd
import os
import tempfile
from datetime import datetime
import traceback
from werkzeug.utils import secure_filename
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Configure upload settings
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB max file size
UPLOAD_FOLDER = tempfile.mkdtemp()
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

class SiteDataConsolidator:
    def __init__(self):
        self.disconnected_df = None
        self.site_details_df = None
        self.merged_df = None
        
    def load_excel_files(self, disconnected_file_path, site_details_file_path):
        """Load the two Excel files"""
        try:
            # Load the disconnected sites file
            self.disconnected_df = pd.read_excel(disconnected_file_path)
            logger.info(f"Loaded disconnected sites file: {len(self.disconnected_df)} rows")
            logger.info(f"Disconnected file columns: {list(self.disconnected_df.columns)}")
            
            # Load the site details file
            self.site_details_df = pd.read_excel(site_details_file_path)
            logger.info(f"Loaded site details file: {len(self.site_details_df)} rows")
            logger.info(f"Site details file columns: {list(self.site_details_df.columns)}")
            
            return True
        except Exception as e:
            logger.error(f"Error loading Excel files: {str(e)}")
            return False
    
    def clean_site_names(self):
        """Clean site names for better matching"""
        if self.disconnected_df is not None and self.site_details_df is not None:
            disconnected_site_col = self.get_site_name_column(self.disconnected_df)
            details_site_col = self.get_site_name_column(self.site_details_df)
            
            if disconnected_site_col and details_site_col:
                self.disconnected_df[f'{disconnected_site_col}_clean'] = (
                    self.disconnected_df[disconnected_site_col]
                    .astype(str)
                    .str.strip()
                    .str.lower()
                    .str.replace(r'[^\w\s]', '', regex=True)  # Remove special characters
                )
                
                self.site_details_df[f'{details_site_col}_clean'] = (
                    self.site_details_df[details_site_col]
                    .astype(str)
                    .str.strip()
                    .str.lower()
                    .str.replace(r'[^\w\s]', '', regex=True)  # Remove special characters
                )
    
    def get_site_name_column(self, df):
        """Automatically detect site name column"""
        possible_names = ['site name', 'site_name', 'sitename', 'site', 'name', 'location']
        df_columns_lower = [col.lower() for col in df.columns]
        
        for name in possible_names:
            for i, col in enumerate(df_columns_lower):
                if name in col:
                    return df.columns[i]
        return None
    
    def get_disconnection_column(self, df):
        """Automatically detect disconnection status column"""
        possible_names = ['disconnected', 'disconnect', 'status', 'connection', 'state', 'conn_status']
        df_columns_lower = [col.lower() for col in df.columns]
        
        for name in possible_names:
            for i, col in enumerate(df_columns_lower):
                if name in col:
                    return df.columns[i]
        return None
    
    def get_column_by_keywords(self, df, keywords):
        """Get column name that contains any of the keywords"""
        df_columns_lower = [col.lower() for col in df.columns]
        
        for keyword in keywords:
            for i, col in enumerate(df_columns_lower):
                if keyword.lower() in col:
                    return df.columns[i]
        return None
    
    def filter_disconnected_sites(self):
        """Filter only the disconnected sites from the first file"""
        if self.disconnected_df is None:
            return False, "Disconnected sites data not loaded"
        
        disconnect_col = self.get_disconnection_column(self.disconnected_df)
        if not disconnect_col:
            # If no disconnection column found, assume all sites in the file are disconnected
            logger.warning("No disconnection status column found. Assuming all sites are disconnected.")
            return True, f"Processing all {len(self.disconnected_df)} sites (no status column found)"
        
        # Filter for disconnected sites (case-insensitive)
        disconnected_mask = (
            self.disconnected_df[disconnect_col]
            .astype(str)
            .str.lower()
            .str.contains('disconnect|down|offline|not connected', na=False, regex=True)
        )
        
        original_count = len(self.disconnected_df)
        self.disconnected_df = self.disconnected_df[disconnected_mask]
        filtered_count = len(self.disconnected_df)
        
        logger.info(f"Filtered {filtered_count} disconnected sites from {original_count} total sites")
        return True, f"Found {filtered_count} disconnected sites out of {original_count} total sites"
    
    def merge_data(self):
        """Merge the two dataframes based on site names"""
        if self.disconnected_df is None or self.site_details_df is None:
            return False, "Both files must be loaded first"
        
        # Clean site names for better matching
        self.clean_site_names()
        
        # Get column names
        disconnected_site_col = self.get_site_name_column(self.disconnected_df)
        details_site_col = self.get_site_name_column(self.site_details_df)
        
        if not disconnected_site_col or not details_site_col:
            return False, f"Could not find site name columns. Disconnected file columns: {list(self.disconnected_df.columns)}, Details file columns: {list(self.site_details_df.columns)}"
        
        logger.info(f"Merging on: {disconnected_site_col} (disconnected) <-> {details_site_col} (details)")
        
        # Merge dataframes using both original and cleaned names for better matching
        self.merged_df = pd.merge(
            self.disconnected_df,
            self.site_details_df,
            left_on=f'{disconnected_site_col}_clean',
            right_on=f'{details_site_col}_clean',
            how='left',
            suffixes=('_disconnected', '_details')
        )
        
        matched_count = self.merged_df[f'{details_site_col}_clean'].notna().sum()
        total_count = len(self.merged_df)
        
        logger.info(f"Merged data: {total_count} rows, {matched_count} successfully matched")
        return True, f"Merged data: {total_count} rows, {matched_count} successfully matched with site details"
    
    def create_consolidated_report(self, output_file_path):
        """Create the final consolidated Excel report"""
        if self.merged_df is None:
            return False, "Data must be merged first"
        
        try:
            # Identify required columns from both files
            site_col = self.get_site_name_column(self.disconnected_df)
            scheme_col = self.get_column_by_keywords(self.disconnected_df, ['scheme', 'scheme_id', 'schemeid'])
            rtu_col = self.get_column_by_keywords(self.site_details_df, ['rtu', 'rtu_id', 'rtuid'])
            ovpn_col = self.get_column_by_keywords(self.site_details_df, ['ovpn', 'ip', 'address', 'ovpn_ip'])
            agency_col = self.get_column_by_keywords(self.site_details_df, ['agency', 'department', 'org'])
            
            logger.info(f"Identified columns - Site: {site_col}, Scheme: {scheme_col}, RTU: {rtu_col}, OVPN: {ovpn_col}, Agency: {agency_col}")
            
            # Create final report with required columns
            report_data = {}
            
            if site_col:
                # Use the original site name from disconnected file
                report_data['Site Name'] = self.merged_df[site_col]
            
            if scheme_col:
                report_data['Scheme ID'] = self.merged_df[scheme_col]
            
            if rtu_col:
                # Check if column exists with suffix (from merge)
                if rtu_col in self.merged_df.columns:
                    report_data['RTU ID'] = self.merged_df[rtu_col]
                elif f'{rtu_col}_details' in self.merged_df.columns:
                    report_data['RTU ID'] = self.merged_df[f'{rtu_col}_details']
            
            if ovpn_col:
                # Check if column exists with suffix (from merge)
                if ovpn_col in self.merged_df.columns:
                    report_data['OVPN IP Address'] = self.merged_df[ovpn_col]
                elif f'{ovpn_col}_details' in self.merged_df.columns:
                    report_data['OVPN IP Address'] = self.merged_df[f'{ovpn_col}_details']
            
            if agency_col:
                # Check if column exists with suffix (from merge)
                if agency_col in self.merged_df.columns:
                    report_data['Agency'] = self.merged_df[agency_col]
                elif f'{agency_col}_details' in self.merged_df.columns:
                    report_data['Agency'] = self.merged_df[f'{agency_col}_details']
            
            # If no specific columns found, include all important columns
            if not report_data:
                logger.warning("No specific columns found, including all columns")
                report_data = self.merged_df.to_dict('series')
            
            # Create final dataframe
            final_df = pd.DataFrame(report_data)
            
            # Remove rows with all NaN values
            final_df = final_df.dropna(how='all')
            
            # Remove duplicate columns and clean up
            final_df = final_df.loc[:, ~final_df.columns.duplicated()]
            
            # Save to Excel with better formatting
            with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
                final_df.to_excel(writer, sheet_name='Disconnected Sites Report', index=False)
                
                # Get the worksheet
                worksheet = writer.sheets['Disconnected Sites Report']
                
                # Auto-adjust columns width
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
            
            summary = {
                'total_sites': len(final_df),
                'columns': list(final_df.columns),
                'column_stats': {}
            }
            
            for col in final_df.columns:
                non_null_count = final_df[col].notna().sum()
                summary['column_stats'][col] = f"{non_null_count}/{len(final_df)}"
            
            logger.info(f"Created consolidated report with {len(final_df)} sites")
            return True, summary
            
        except Exception as e:
            logger.error(f"Error creating consolidated report: {str(e)}")
            logger.error(traceback.format_exc())
            return False, f"Error creating consolidated report: {str(e)}"

@app.route('/')
def index():
    return '''
    <!DOCTYPE html>
    <html>
    <head>
        <title>Site Data Consolidation API</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; background: #f5f5f5; }
            .container { background: white; padding: 30px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
            h1 { color: #333; border-bottom: 2px solid #007bff; padding-bottom: 10px; }
            .endpoint { background: #f8f9fa; padding: 15px; margin: 10px 0; border-radius: 5px; border-left: 4px solid #007bff; }
            .method { background: #007bff; color: white; padding: 3px 8px; border-radius: 3px; font-size: 12px; }
            ul { list-style-type: none; padding: 0; }
            li { margin: 8px 0; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🔧 Site Data Consolidation API</h1>
            <p>Upload two Excel files to consolidate disconnected site data automatically.</p>
            
            <h3>📡 Available Endpoints:</h3>
            <div class="endpoint">
                <span class="method">POST</span> <strong>/process</strong> - Upload and process Excel files
                <ul>
                    <li>• Upload 'disconnected_file' and 'site_details_file'</li>
                    <li>• Returns download link for consolidated report</li>
                </ul>
            </div>
            
            <div class="endpoint">
                <span class="method">GET</span> <strong>/download/&lt;filename&gt;</strong> - Download processed file
            </div>
            
            <div class="endpoint">
                <span class="method">GET</span> <strong>/health</strong> - Server health check
            </div>
            
            <h3>📋 What this API does:</h3>
            <ul>
                <li>✅ Automatically detects column names (Site Name, Scheme ID, RTU ID, OVPN IP, Agency)</li>
                <li>✅ Filters disconnected sites from the first file</li>
                <li>✅ Matches sites by name between both files</li>
                <li>✅ Creates consolidated Excel report with all required data</li>
                <li>✅ Handles various Excel formats (.xlsx, .xls)</li>
                <li>✅ Provides detailed processing statistics</li>
            </ul>
            
            <p><strong>Frontend:</strong> Use the provided HTML frontend to interact with this API easily.</p>
        </div>
    </body>
    </html>
    '''

@app.route('/process', methods=['POST'])
def process_files():
    try:
        # Check if files are present
        if 'disconnected_file' not in request.files or 'site_details_file' not in request.files:
            return jsonify({
                'success': False,
                'message': 'Both disconnected_file and site_details_file are required'
            }), 400
        
        disconnected_file = request.files['disconnected_file']
        site_details_file = request.files['site_details_file']
        
        # Check if files are selected
        if disconnected_file.filename == '' or site_details_file.filename == '':
            return jsonify({
                'success': False,
                'message': 'Please select both files'
            }), 400
        
        # Check file extensions
        allowed_extensions = {'.xlsx', '.xls'}
        if not any(disconnected_file.filename.lower().endswith(ext) for ext in allowed_extensions):
            return jsonify({
                'success': False,
                'message': 'Disconnected file must be an Excel file (.xlsx or .xls)'
            }), 400
        
        if not any(site_details_file.filename.lower().endswith(ext) for ext in allowed_extensions):
            return jsonify({
                'success': False,
                'message': 'Site details file must be an Excel file (.xlsx or .xls)'
            }), 400
        
        # Save uploaded files
        disconnected_filename = secure_filename(disconnected_file.filename)
        site_details_filename = secure_filename(site_details_file.filename)
        
        disconnected_path = os.path.join(app.config['UPLOAD_FOLDER'], disconnected_filename)
        site_details_path = os.path.join(app.config['UPLOAD_FOLDER'], site_details_filename)
        
        disconnected_file.save(disconnected_path)
        site_details_file.save(site_details_path)
        
        logger.info(f"Processing files: {disconnected_filename} and {site_details_filename}")
        
        # Process files
        consolidator = SiteDataConsolidator()
        
        # Load files
        if not consolidator.load_excel_files(disconnected_path, site_details_path):
            return jsonify({
                'success': False,
                'message': 'Failed to load Excel files. Please check file format and content.'
            }), 400
        
        # Filter disconnected sites
        success, message = consolidator.filter_disconnected_sites()
        if not success:
            return jsonify({
                'success': False,
                'message': message
            }), 400
        
        # Merge data
        success, merge_message = consolidator.merge_data()
        if not success:
            return jsonify({
                'success': False,
                'message': merge_message
            }), 400
        
        # Create output file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"consolidated_disconnected_sites_{timestamp}.xlsx"
        output_path = os.path.join(app.config['UPLOAD_FOLDER'], output_filename)
        
        # Generate report
        success, result = consolidator.create_consolidated_report(output_path)
        if not success:
            return jsonify({
                'success': False,
                'message': result
            }), 400
        
        # Clean up input files
        try:
            os.remove(disconnected_path)
            os.remove(site_details_path)
        except Exception as e:
            logger.warning(f"Could not clean up input files: {e}")
        
        return jsonify({
            'success': True,
            'message': f'Files processed successfully! {message}. {merge_message}',
            'download_url': f'/download/{output_filename}',
            'filename': output_filename,
            'summary': result
        })
        
    except Exception as e:
        logger.error(f"Error processing files: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            'success': False,
            'message': f'Error processing files: {str(e)}'
        }), 500

@app.route('/download/<filename>')
def download_file(filename):
    try:
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        if not os.path.exists(file_path):
            return jsonify({'error': 'File not found'}), 404
        
        return send_file(
            file_path,
            as_attachment=True,
            download_name=filename,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    except Exception as e:
        logger.error(f"Error downloading file: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/health')
def health_check():
    return jsonify({
        'status': 'healthy', 
        'timestamp': datetime.now().isoformat(),
        'upload_folder': app.config['UPLOAD_FOLDER'],
        'max_file_size': '50MB'
    })

# Clean up old files periodically
def cleanup_old_files():
    """Clean up files older than 1 hour"""
    try:
        current_time = datetime.now().timestamp()
        for filename in os.listdir(app.config['UPLOAD_FOLDER']):
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            if os.path.isfile(file_path):
                file_age = current_time - os.path.getmtime(file_path)
                if file_age > 3600:  # 1 hour
                    os.remove(file_path)
                    logger.info(f"Cleaned up old file: {filename}")
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")

if __name__ == '__main__':
    print("=" * 60)
    print("🚀 Starting Site Data Consolidation Server...")
    print("=" * 60)
    print(f"📁 Upload folder: {UPLOAD_FOLDER}")
    print("🌐 Server URL: http://localhost:5000")
    print("📄 API Documentation: http://localhost:5000")
    print("=" * 60)
    print("💡 Instructions:")
    print("1. Keep this terminal open")
    print("2. Open index.html in your web browser")
    print("3. Upload your two Excel files")
    print("4. Download the consolidated report")
    print("=" * 60)
    
    # Clean up old files before starting
    cleanup_old_files()
    
    app.run(debug=True, host='0.0.0.0', port=5000)