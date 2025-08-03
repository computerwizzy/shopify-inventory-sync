import pandas as pd
import streamlit as st
from io import StringIO
import chardet
import os

class FileProcessor:
    """Handles processing of uploaded CSV and Excel files."""
    
    def __init__(self):
        self.supported_extensions = ['.csv', '.xlsx', '.xls']
    
    def process_file(self, uploaded_file):
        """
        Process an uploaded file and return a pandas DataFrame.
        
        Args:
            uploaded_file: Streamlit uploaded file object
            
        Returns:
            pandas.DataFrame: Processed data
            
        Raises:
            ValueError: If file format is not supported
            Exception: If file cannot be processed
        """
        file_extension = self._get_file_extension(uploaded_file.name)
        
        if file_extension not in self.supported_extensions:
            raise ValueError(f"Unsupported file format: {file_extension}")
        
        try:
            if file_extension == '.csv':
                return self._process_csv(uploaded_file)
            elif file_extension in ['.xlsx', '.xls']:
                return self._process_excel(uploaded_file)
        except Exception as e:
            raise Exception(f"Error processing file: {str(e)}")
    
    def _get_file_extension(self, filename):
        """Extract file extension from filename."""
        return '.' + filename.split('.')[-1].lower()
    
    def _process_csv(self, uploaded_file):
        """
        Process CSV file with automatic encoding detection.
        
        Args:
            uploaded_file: Streamlit uploaded file object
            
        Returns:
            pandas.DataFrame: Processed CSV data
        """
        # Read file content
        content = uploaded_file.read()
        
        # Detect encoding
        encoding_result = chardet.detect(content)
        encoding = encoding_result.get('encoding', 'utf-8')
        
        # Fallback encodings to try
        encodings_to_try = [encoding, 'utf-8', 'latin1', 'cp1252']
        
        for enc in encodings_to_try:
            try:
                # Try to decode content with current encoding
                content_str = content.decode(enc)
                df = pd.read_csv(StringIO(content_str))
                
                # Clean column names
                df.columns = self._clean_column_names(df.columns)
                
                # Remove empty rows
                df = df.dropna(how='all')
                
                return df
                
            except UnicodeDecodeError:
                continue
            except Exception as e:
                if enc == encodings_to_try[-1]:  # Last encoding attempt
                    raise Exception(f"Failed to read CSV file: {str(e)}")
                continue
        
        raise Exception("Could not decode the CSV file with any supported encoding")
    
    def _process_excel(self, uploaded_file):
        """
        Process Excel file (.xlsx or .xls).
        
        Args:
            uploaded_file: Streamlit uploaded file object
            
        Returns:
            pandas.DataFrame: Processed Excel data
        """
        try:
            # Read Excel file
            df = pd.read_excel(uploaded_file, engine='openpyxl')
            
            # Clean column names
            df.columns = self._clean_column_names(df.columns)
            
            # Remove empty rows
            df = df.dropna(how='all')
            
            return df
            
        except Exception as e:
            raise Exception(f"Failed to read Excel file: {str(e)}")
    
    def _clean_column_names(self, columns):
        """
        Clean column names by removing extra spaces and special characters.
        
        Args:
            columns: List or Index of column names
            
        Returns:
            List: Cleaned column names
        """
        cleaned = []
        for col in columns:
            # Convert to string and strip whitespace
            clean_col = str(col).strip()
            
            # Replace multiple spaces with single space
            clean_col = ' '.join(clean_col.split())
            
            # Remove common prefixes/suffixes that might cause issues
            if clean_col.startswith('Unnamed:'):
                clean_col = f"Column_{len(cleaned) + 1}"
            
            cleaned.append(clean_col)
        
        return cleaned
    
    def validate_data(self, df, required_columns=None):
        """
        Validate the processed data.
        
        Args:
            df: pandas.DataFrame to validate
            required_columns: List of required column names (optional)
            
        Returns:
            dict: Validation results with 'valid' boolean and 'messages' list
        """
        validation_result = {
            'valid': True,
            'messages': []
        }
        
        # Check if DataFrame is empty
        if df.empty:
            validation_result['valid'] = False
            validation_result['messages'].append("File appears to be empty")
            return validation_result
        
        # Check for required columns
        if required_columns:
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                validation_result['valid'] = False
                validation_result['messages'].append(f"Missing required columns: {', '.join(missing_columns)}")
        
        # Check for duplicate column names
        if len(df.columns) != len(set(df.columns)):
            validation_result['messages'].append("Warning: Duplicate column names detected")
        
        # Check data types and suggest optimizations
        numeric_columns = df.select_dtypes(include=['object']).columns
        for col in numeric_columns:
            # Try to convert to numeric
            try:
                pd.to_numeric(df[col], errors='raise')
                validation_result['messages'].append(f"Column '{col}' appears to contain numeric data but is stored as text")
            except:
                pass  # Column contains non-numeric data, which is fine
        
        return validation_result
    
    def process_file_by_path(self, file_path: str):
        """
        Process a file by file path (for scheduled tasks).
        
        Args:
            file_path: Local file path
            
        Returns:
            pandas.DataFrame: Processed data
        """
        file_extension = self._get_file_extension(os.path.basename(file_path))
        
        if file_extension not in self.supported_extensions:
            raise ValueError(f"Unsupported file format: {file_extension}")
        
        try:
            if file_extension == '.csv':
                return self._process_csv_file(file_path)
            elif file_extension in ['.xlsx', '.xls']:
                return self._process_excel_file(file_path)
        except Exception as e:
            raise Exception(f"Error processing file: {str(e)}")
    
    def _process_csv_file(self, file_path: str):
        """Process CSV file by path."""
        encodings_to_try = ['utf-8', 'latin1', 'cp1252']
        
        for encoding in encodings_to_try:
            try:
                df = pd.read_csv(file_path, encoding=encoding)
                df.columns = self._clean_column_names(df.columns)
                df = df.dropna(how='all')
                return df
            except UnicodeDecodeError:
                continue
            except Exception as e:
                if encoding == encodings_to_try[-1]:
                    raise Exception(f"Failed to read CSV file: {str(e)}")
                continue
        
        raise Exception("Could not decode the CSV file with any supported encoding")
    
    def _process_excel_file(self, file_path: str):
        """Process Excel file by path."""
        try:
            df = pd.read_excel(file_path, engine='openpyxl')
            df.columns = self._clean_column_names(df.columns)
            df = df.dropna(how='all')
            return df
        except Exception as e:
            raise Exception(f"Failed to read Excel file: {str(e)}")

    def get_file_info(self, df, uploaded_file):
        """
        Get information about the processed file.
        
        Args:
            df: pandas.DataFrame
            uploaded_file: Streamlit uploaded file object
            
        Returns:
            dict: File information
        """
        return {
            'filename': uploaded_file.name,
            'size_bytes': uploaded_file.size,
            'size_kb': round(uploaded_file.size / 1024, 2),
            'rows': len(df),
            'columns': len(df.columns),
            'column_names': df.columns.tolist(),
            'memory_usage_mb': round(df.memory_usage(deep=True).sum() / 1024 / 1024, 2),
            'dtypes': df.dtypes.to_dict()
        }
    
    def filter_selected_columns(self, df, selected_columns=None):
        """
        Filter DataFrame to include only selected columns.
        
        Args:
            df: pandas.DataFrame to filter
            selected_columns: List of column names to include (None = include all)
            
        Returns:
            pandas.DataFrame: Filtered DataFrame
        """
        if not selected_columns:
            return df
        
        # Find columns that exist in both the DataFrame and selected_columns
        existing_columns = [col for col in selected_columns if col in df.columns]
        
        if not existing_columns:
            # If no selected columns exist, return all columns (fallback)
            return df
        
        # Return DataFrame with only selected columns
        return df[existing_columns]