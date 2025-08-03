import ftplib
import paramiko
import requests
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import io
import os
from typing import Dict, List, Optional, Union
import tempfile
import json
from datetime import datetime
import streamlit as st

class FeedSourceManager:
    """Manages various feed sources for inventory data."""
    
    def __init__(self):
        self.temp_dir = tempfile.gettempdir()
    
    def download_from_ftp(self, host: str, username: str, password: str, 
                         file_path: str, port: int = 21) -> str:
        """
        Download file from FTP server.
        
        Args:
            host: FTP server host
            username: FTP username
            password: FTP password
            file_path: Path to file on FTP server
            port: FTP port (default 21)
            
        Returns:
            str: Local file path of downloaded file
            
        Raises:
            Exception: If FTP download fails
        """
        ftp = None
        try:
            # Create FTP connection
            ftp = ftplib.FTP()
            ftp.connect(host, port)
            ftp.login(username, password)
            
            # Generate local file path
            filename = os.path.basename(file_path)
            local_path = os.path.join(self.temp_dir, f"ftp_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{filename}")
            
            # Download file
            with open(local_path, 'wb') as local_file:
                ftp.retrbinary(f'RETR {file_path}', local_file.write)
            
            return local_path
            
        except Exception as e:
            raise Exception(f"FTP download failed: {str(e)}")
        finally:
            if ftp:
                try:
                    ftp.quit()
                except:
                    ftp.close()
    
    def download_from_sftp(self, host: str, username: str, password: str,
                          file_path: str, port: int = 22, private_key: str = None) -> str:
        """
        Download file from SFTP server.
        
        Args:
            host: SFTP server host
            username: SFTP username
            password: SFTP password (optional if using private key)
            file_path: Path to file on SFTP server
            port: SFTP port (default 22)
            private_key: Private key file path (optional)
            
        Returns:
            str: Local file path of downloaded file
            
        Raises:
            Exception: If SFTP download fails
        """
        try:
            # Create SSH client
            ssh_client = paramiko.SSHClient()
            # Load system host keys for better security
            ssh_client.load_system_host_keys()
            ssh_client.load_host_keys(os.path.expanduser('~/.ssh/known_hosts'))
            # Only accept known hosts for security
            ssh_client.set_missing_host_key_policy(paramiko.RejectPolicy())
            
            # Connect with password or private key
            if private_key and os.path.exists(private_key):
                ssh_client.connect(host, port=port, username=username, key_filename=private_key)
            else:
                ssh_client.connect(host, port=port, username=username, password=password)
            
            # Create SFTP client
            sftp = ssh_client.open_sftp()
            
            # Generate local file path
            filename = os.path.basename(file_path)
            local_path = os.path.join(self.temp_dir, f"sftp_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{filename}")
            
            # Download file
            sftp.get(file_path, local_path)
            
            return local_path
            
        except Exception as e:
            raise Exception(f"SFTP download failed: {str(e)}")
        finally:
            try:
                if 'sftp' in locals():
                    sftp.close()
                if 'ssh_client' in locals():
                    ssh_client.close()
            except:
                pass
    
    def download_from_url(self, url: str, headers: Dict = None, 
                         auth: tuple = None, timeout: int = 30) -> str:
        """
        Download file from URL.
        
        Args:
            url: File URL
            headers: HTTP headers (optional)
            auth: Authentication tuple (username, password) (optional)
            timeout: Request timeout in seconds
            
        Returns:
            str: Local file path of downloaded file
            
        Raises:
            Exception: If URL download fails
        """
        try:
            # Make request
            response = requests.get(url, headers=headers, auth=auth, timeout=timeout, stream=True)
            response.raise_for_status()
            
            # Determine filename from URL or content-disposition
            filename = self._extract_filename_from_url(url, response.headers)
            local_path = os.path.join(self.temp_dir, f"url_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{filename}")
            
            # Download file
            with open(local_path, 'wb') as local_file:
                for chunk in response.iter_content(chunk_size=8192):
                    local_file.write(chunk)
            
            return local_path
            
        except Exception as e:
            raise Exception(f"URL download failed: {str(e)}")
    
    def download_from_google_sheets(self, sheet_id: str, worksheet_name: str = None,
                                  credentials_path: str = None, 
                                  credentials_json: Dict = None) -> pd.DataFrame:
        """
        Download data from Google Sheets.
        
        Args:
            sheet_id: Google Sheets ID
            worksheet_name: Worksheet name (optional, uses first sheet if not provided)
            credentials_path: Path to service account credentials file
            credentials_json: Service account credentials as dict
            
        Returns:
            pd.DataFrame: Sheet data as DataFrame
            
        Raises:
            Exception: If Google Sheets download fails
        """
        try:
            # Setup credentials
            scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly',
                     'https://www.googleapis.com/auth/drive.readonly']
            
            if credentials_json:
                creds = Credentials.from_service_account_info(credentials_json, scopes=scopes)
            elif credentials_path and os.path.exists(credentials_path):
                creds = Credentials.from_service_account_file(credentials_path, scopes=scopes)
            else:
                raise Exception("Google Sheets credentials not provided")
            
            # Connect to Google Sheets
            client = gspread.authorize(creds)
            spreadsheet = client.open_by_key(sheet_id)
            
            # Get worksheet
            if worksheet_name:
                worksheet = spreadsheet.worksheet(worksheet_name)
            else:
                worksheet = spreadsheet.sheet1
            
            # Get all data
            data = worksheet.get_all_records()
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            return df
            
        except Exception as e:
            raise Exception(f"Google Sheets download failed: {str(e)}")
    
    def _extract_filename_from_url(self, url: str, headers: Dict) -> str:
        """Extract filename from URL or response headers."""
        import re
        
        # For Google Sheets, use a simple filename
        if 'docs.google.com/spreadsheets' in url:
            return f"googlesheet_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        # Try content-disposition header first
        if 'content-disposition' in headers:
            content_disposition = headers['content-disposition']
            if 'filename=' in content_disposition:
                # Extract filename and sanitize for Windows
                filename = content_disposition.split('filename=')[1].strip('"').split(';')[0]
                # Remove invalid characters for Windows filenames
                filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
                # Remove trailing underscores and ensure proper extension
                filename = filename.rstrip('_')
                if filename and '.' in filename:
                    return filename
        
        # Extract from URL
        filename = url.split('/')[-1].split('?')[0]
        if not filename or '.' not in filename:
            filename = f"download_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        # Sanitize filename for Windows
        filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
        return filename
    
    def test_ftp_connection(self, host: str, username: str, password: str, 
                           port: int = 21) -> bool:
        """Test FTP connection."""
        try:
            ftp = ftplib.FTP()
            ftp.connect(host, port)
            ftp.login(username, password)
            ftp.quit()
            return True
        except:
            return False
    
    def test_sftp_connection(self, host: str, username: str, password: str,
                            port: int = 22, private_key: str = None) -> bool:
        """Test SFTP connection."""
        try:
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            if private_key and os.path.exists(private_key):
                ssh_client.connect(host, port=port, username=username, key_filename=private_key)
            else:
                ssh_client.connect(host, port=port, username=username, password=password)
            
            ssh_client.close()
            return True
        except:
            return False
    
    def test_url_connection(self, url: str, headers: Dict = None,
                           auth: tuple = None, timeout: int = 10) -> bool:
        """Test URL connection."""
        try:
            # For Google Sheets, use GET instead of HEAD as HEAD might not work
            if 'docs.google.com/spreadsheets' in url:
                response = requests.get(url, headers=headers, auth=auth, timeout=timeout, stream=True)
                # Read just first 1KB to verify it's working without downloading everything
                content = next(response.iter_content(1024), b'')
                return response.status_code == 200 and len(content) > 0
            else:
                # For other URLs, HEAD is fine
                response = requests.head(url, headers=headers, auth=auth, timeout=timeout)
                return response.status_code == 200
        except Exception as e:
            print(f"URL test failed: {e}")  # For debugging
            return False
    
    def get_feed_headers(self, feed_type: str, config: Dict) -> List[str]:
        """
        Get column headers from a feed source without downloading the entire file.
        
        Args:
            feed_type: Type of feed (ftp, sftp, url, google_sheets)
            config: Feed configuration dictionary
            
        Returns:
            List[str]: Column headers from the feed
            
        Raises:
            Exception: If headers cannot be retrieved
        """
        try:
            if feed_type == 'url':
                return self._get_url_headers(config)
            elif feed_type == 'ftp':
                return self._get_ftp_headers(config)
            elif feed_type == 'sftp':
                return self._get_sftp_headers(config)
            elif feed_type == 'google_sheets':
                return self._get_google_sheets_headers(config)
            else:
                raise ValueError(f"Unsupported feed type: {feed_type}")
        except Exception as e:
            raise Exception(f"Failed to get headers: {str(e)}")
    
    def _get_url_headers(self, config: Dict) -> List[str]:
        """Get headers from URL feed by reading first few lines."""
        import csv
        from io import StringIO
        
        response = requests.get(
            config['url'], 
            headers=config.get('headers'),
            auth=tuple(config['auth']) if config.get('auth') else None,
            timeout=config.get('timeout', 30),
            stream=True
        )
        response.raise_for_status()
        
        # Read just the first few KB to get headers
        first_chunk = ""
        for chunk in response.iter_content(chunk_size=1024, decode_unicode=True):
            first_chunk += chunk
            # Look for first newline to get headers
            if '\n' in first_chunk:
                break
        
        # Parse first line as CSV to get headers
        first_line = first_chunk.split('\n')[0]
        reader = csv.reader(StringIO(first_line))
        headers = next(reader)
        
        # Clean headers
        return [header.strip() for header in headers]
    
    def _get_ftp_headers(self, config: Dict) -> List[str]:
        """Get headers from FTP file by downloading just the first few lines."""
        import csv
        from io import StringIO
        
        ftp = None
        try:
            ftp = ftplib.FTP()
            ftp.connect(config['host'], config.get('port', 21))
            ftp.login(config['username'], config['password'])
            
            # Get first few lines only
            lines = []
            def store_line(line):
                lines.append(line)
                if len(lines) >= 2:  # Get header + one data line for validation
                    raise StopIteration  # Break out of retrlines
            
            try:
                ftp.retrlines(f'RETR {config["file_path"]}', store_line)
            except StopIteration:
                pass  # Expected when we have enough lines
            
            if not lines:
                raise Exception("No data found in FTP file")
            
            # Parse first line as CSV
            reader = csv.reader(StringIO(lines[0]))
            headers = next(reader)
            return [header.strip() for header in headers]
            
        finally:
            if ftp:
                try:
                    ftp.quit()
                except:
                    ftp.close()
    
    def _get_sftp_headers(self, config: Dict) -> List[str]:
        """Get headers from SFTP file by downloading just the beginning."""
        import csv
        from io import StringIO
        
        ssh_client = None
        try:
            ssh_client = paramiko.SSHClient()
            ssh_client.load_system_host_keys()
            ssh_client.load_host_keys(os.path.expanduser('~/.ssh/known_hosts'))
            ssh_client.set_missing_host_key_policy(paramiko.RejectPolicy())
            
            # Connect
            if config.get('private_key') and os.path.exists(config['private_key']):
                ssh_client.connect(
                    config['host'], 
                    port=config.get('port', 22), 
                    username=config['username'], 
                    key_filename=config['private_key']
                )
            else:
                ssh_client.connect(
                    config['host'], 
                    port=config.get('port', 22), 
                    username=config['username'], 
                    password=config.get('password')
                )
            
            # Read first few lines using head command
            stdin, stdout, stderr = ssh_client.exec_command(f'head -n 2 {config["file_path"]}')
            lines = stdout.read().decode('utf-8').strip().split('\n')
            
            if not lines or not lines[0]:
                raise Exception("No data found in SFTP file")
            
            # Parse first line as CSV
            reader = csv.reader(StringIO(lines[0]))
            headers = next(reader)
            return [header.strip() for header in headers]
            
        finally:
            if ssh_client:
                ssh_client.close()
    
    def _get_google_sheets_headers(self, config: Dict) -> List[str]:
        """Get headers from Google Sheets."""
        # For Google Sheets API access, we'd need credentials
        # For public sheets via URL, we can use the URL method
        if 'url' in config:
            return self._get_url_headers(config)
        else:
            # Use actual Google Sheets API if credentials are provided
            df = self.download_from_google_sheets(
                config['sheet_id'],
                config.get('worksheet_name'),
                config.get('credentials_path'),
                config.get('credentials_json')
            )
            return df.columns.tolist()
    
    def test_google_sheets_connection(self, sheet_id: str, credentials_path: str = None,
                                    credentials_json: Dict = None) -> bool:
        """Test Google Sheets connection."""
        try:
            scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']
            
            if credentials_json:
                creds = Credentials.from_service_account_info(credentials_json, scopes=scopes)
            elif credentials_path and os.path.exists(credentials_path):
                creds = Credentials.from_service_account_file(credentials_path, scopes=scopes)
            else:
                return False
            
            client = gspread.authorize(creds)
            spreadsheet = client.open_by_key(sheet_id)
            return True
        except:
            return False

class FeedConfigManager:
    """Manages feed source configurations."""
    
    def __init__(self, config_file: str = "feed_configs.json"):
        self.config_file = config_file
        self.configs = self.load_configs()
    
    def load_configs(self) -> Dict:
        """Load feed configurations from file."""
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r') as f:
                    return json.load(f)
            except:
                return {}
        return {}
    
    def save_configs(self) -> None:
        """Save feed configurations to file."""
        try:
            with open(self.config_file, 'w') as f:
                json.dump(self.configs, f, indent=2, default=str)
        except Exception as e:
            st.error(f"Failed to save feed configs: {str(e)}")
    
    def add_config(self, name: str, config: Dict) -> None:
        """Add a new feed configuration."""
        config['created_at'] = datetime.now().isoformat()
        config['updated_at'] = datetime.now().isoformat()
        self.configs[name] = config
        self.save_configs()
    
    def update_config(self, name: str, config: Dict) -> None:
        """Update an existing feed configuration."""
        if name in self.configs:
            config['created_at'] = self.configs[name].get('created_at', datetime.now().isoformat())
            config['updated_at'] = datetime.now().isoformat()
            self.configs[name] = config
            self.save_configs()
    
    def delete_config(self, name: str) -> None:
        """Delete a feed configuration."""
        if name in self.configs:
            del self.configs[name]
            self.save_configs()
    
    def get_config(self, name: str) -> Optional[Dict]:
        """Get a feed configuration by name."""
        return self.configs.get(name)
    
    def list_configs(self) -> List[str]:
        """List all feed configuration names."""
        return list(self.configs.keys())
    
    def get_configs_by_type(self, feed_type: str) -> Dict:
        """Get all configurations of a specific type."""
        return {name: config for name, config in self.configs.items() 
                if config.get('type') == feed_type}