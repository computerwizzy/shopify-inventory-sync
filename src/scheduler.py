from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
import logging
import json
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Callable
import pandas as pd
import streamlit as st
from src.feed_sources import FeedSourceManager, FeedConfigManager
from src.file_processor import FileProcessor
from src.column_mapper import ColumnMapper
from src.sku_matcher import SKUMatcher
from src.shopify_client import ShopifyClient
from utils.config import Config

class SyncScheduler:
    """Manages scheduled inventory synchronization tasks."""
    
    def __init__(self):
        self.scheduler = BackgroundScheduler()
        self.scheduler.start()
        self.config_manager = FeedConfigManager()
        self.feed_manager = FeedSourceManager()
        self.config = Config()
        
        # Setup logging
        self.setup_logging()
        
        # Add event listeners
        self.scheduler.add_listener(self.job_executed, EVENT_JOB_EXECUTED)
        self.scheduler.add_listener(self.job_error, EVENT_JOB_ERROR)
        
        # Load existing scheduled jobs
        self.load_scheduled_jobs()
    
    def setup_logging(self):
        """Setup logging for scheduled tasks."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('sync_scheduler.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger('SyncScheduler')
    
    def add_scheduled_sync(self, job_id: str, feed_config_name: str, 
                          schedule_type: str, schedule_config: Dict,
                          column_mapping: Dict = None, sync_fields: Dict = None,
                          sync_options: Dict = None) -> bool:
        """
        Add a scheduled sync job.
        
        Args:
            job_id: Unique job identifier
            feed_config_name: Name of feed configuration to use
            schedule_type: 'cron' or 'interval'
            schedule_config: Schedule configuration parameters
            column_mapping: Column mapping configuration
            
        Returns:
            bool: True if job added successfully
        """
        try:
            # Validate feed configuration exists
            feed_config = self.config_manager.get_config(feed_config_name)
            if not feed_config:
                raise ValueError(f"Feed configuration '{feed_config_name}' not found")
            
            # Create job data
            job_data = {
                'job_id': job_id,
                'feed_config_name': feed_config_name,
                'column_mapping': column_mapping or {},
                'sync_fields': sync_fields or {},
                'sync_options': sync_options or {},
                'schedule_type': schedule_type,
                'schedule_config': schedule_config,
                'created_at': datetime.now().isoformat(),
                'last_run': None,
                'last_success': None,
                'last_error': None,
                'run_count': 0,
                'success_count': 0,
                'error_count': 0
            }
            
            # Create trigger based on schedule type
            if schedule_type == 'cron':
                trigger = CronTrigger(**schedule_config)
            elif schedule_type == 'interval':
                trigger = IntervalTrigger(**schedule_config)
            else:
                raise ValueError(f"Invalid schedule type: {schedule_type}")
            
            # Add job to scheduler
            self.scheduler.add_job(
                func=self.execute_sync_job,
                trigger=trigger,
                args=[job_data],
                id=job_id,
                name=f"Sync Job: {job_id}",
                replace_existing=True
            )
            
            # Save job configuration
            self.save_job_config(job_id, job_data)
            
            self.logger.info(f"Scheduled sync job '{job_id}' added successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to add scheduled sync job '{job_id}': {str(e)}")
            return False
    
    def remove_scheduled_sync(self, job_id: str) -> bool:
        """Remove a scheduled sync job."""
        try:
            self.scheduler.remove_job(job_id)
            self.delete_job_config(job_id)
            self.logger.info(f"Scheduled sync job '{job_id}' removed successfully")
            return True
        except Exception as e:
            self.logger.error(f"Failed to remove scheduled sync job '{job_id}': {str(e)}")
            return False
    
    def execute_sync_job(self, job_data: Dict) -> Dict:
        """
        Execute a sync job.
        
        Args:
            job_data: Job configuration data
            
        Returns:
            Dict: Execution result
        """
        job_id = job_data['job_id']
        result = {
            'job_id': job_id,
            'start_time': datetime.now(),
            'success': False,
            'error': None,
            'records_processed': 0,
            'records_synced': 0
        }
        
        try:
            self.logger.info(f"Starting sync job '{job_id}'")
            
            # Update job stats
            job_data['last_run'] = datetime.now().isoformat()
            job_data['run_count'] = job_data.get('run_count', 0) + 1
            
            # Get feed configuration
            feed_config = self.config_manager.get_config(job_data['feed_config_name'])
            if not feed_config:
                raise Exception(f"Feed configuration '{job_data['feed_config_name']}' not found")
            
            # Download data based on feed type
            df = self.download_feed_data(feed_config)
            result['records_processed'] = len(df)
            
            if df.empty:
                raise Exception("No data found in feed")
            
            # Filter to selected columns if configured
            if feed_config.get('selected_columns'):
                processor = FileProcessor()
                df = processor.filter_selected_columns(df, feed_config['selected_columns'])
                self.logger.info(f"Filtered to {len(df.columns)} selected columns: {list(df.columns)}")
            
            # Apply column mapping from feed configuration or job data
            column_mapping = {}
            
            # First, try to get column mapping from feed configuration
            if feed_config.get('column_mapping'):
                column_mapping = feed_config['column_mapping']
                self.logger.info(f"Using column mapping from feed configuration: {column_mapping}")
            
            # Override with job-specific mapping if provided
            if job_data.get('column_mapping'):
                column_mapping.update(job_data['column_mapping'])
                self.logger.info(f"Applied job-specific column mapping override: {job_data['column_mapping']}")
            
            if column_mapping:
                mapper = ColumnMapper(df.columns.tolist())
                df = mapper.get_mapped_data(df, column_mapping)
                self.logger.info(f"Applied column mapping to {len(df)} rows")
            
            # Validate required columns
            if 'SKU' not in df.columns or 'Quantity' not in df.columns:
                raise Exception("Required columns (SKU, Quantity) not found after mapping")
            
            # Initialize Shopify client and perform sync
            if self.config.validate_shopify_config():
                shopify_client = None
                try:
                    shopify_client = ShopifyClient()
                    sku_matcher = SKUMatcher(shopify_client)
                    
                    # Get Shopify products with resilient retry
                    shopify_products = shopify_client.get_all_products()
                    
                    # Match SKUs
                    matched_data = sku_matcher.match_skus(df, column_mapping, shopify_products)
                    
                    # Filter for exact matches only in automated sync
                    sync_data = [m for m in matched_data if m['match_type'] == 'exact']
                    
                    # Get sync fields and options from job data
                    sync_fields = job_data.get('sync_fields', {'inventory_quantity': True})
                    sync_options = job_data.get('sync_options', {})
                    
                    # Perform sync with enhanced error handling
                    sync_results = self.perform_batch_sync(shopify_client, sync_data, sync_fields, sync_options)
                    result['records_synced'] = len([r for r in sync_results if r['success']])
                    
                    # Log API statistics for monitoring
                    api_stats = shopify_client.get_api_stats()
                    self.logger.info(f"API Stats for job '{job_id}': {api_stats}")
                    
                    self.logger.info(f"Sync job '{job_id}' completed: {result['records_synced']}/{len(sync_data)} records synced")
                
                except Exception as api_error:
                    # Enhanced error handling for API issues
                    error_msg = str(api_error)
                    if "temporarily unavailable" in error_msg.lower():
                        self.logger.warning(f"API temporarily unavailable for job '{job_id}'. Will retry in next scheduled run.")
                        # Don't mark as complete failure - let it retry next time
                        result['error'] = f"API temporarily unavailable: {error_msg}"
                    else:
                        raise api_error
                
                finally:
                    if shopify_client:
                        try:
                            shopify_client.close()
                        except:
                            pass
            else:
                raise Exception("Shopify configuration is not valid")
            
            # Mark as successful
            result['success'] = True
            job_data['last_success'] = datetime.now().isoformat()
            job_data['success_count'] = job_data.get('success_count', 0) + 1
            job_data['last_error'] = None
            
        except Exception as e:
            error_msg = str(e)
            result['error'] = error_msg
            job_data['last_error'] = error_msg
            job_data['error_count'] = job_data.get('error_count', 0) + 1
            self.logger.error(f"Sync job '{job_id}' failed: {error_msg}")
        
        finally:
            result['end_time'] = datetime.now()
            result['duration'] = (result['end_time'] - result['start_time']).total_seconds()
            
            # Update job configuration
            self.save_job_config(job_id, job_data)
            
            # Log execution result
            self.log_job_execution(result)
        
        return result
    
    def download_feed_data(self, feed_config: Dict) -> pd.DataFrame:
        """Download data from configured feed source."""
        feed_type = feed_config.get('type')
        
        if feed_type == 'ftp':
            file_path = self.feed_manager.download_from_ftp(
                host=feed_config['host'],
                username=feed_config['username'],
                password=feed_config['password'],
                file_path=feed_config['file_path'],
                port=feed_config.get('port', 21)
            )
            processor = FileProcessor()
            return processor.process_file_by_path(file_path)
            
        elif feed_type == 'sftp':
            file_path = self.feed_manager.download_from_sftp(
                host=feed_config['host'],
                username=feed_config['username'],
                password=feed_config.get('password'),
                file_path=feed_config['file_path'],
                port=feed_config.get('port', 22),
                private_key=feed_config.get('private_key')
            )
            processor = FileProcessor()
            return processor.process_file_by_path(file_path)
            
        elif feed_type == 'url':
            file_path = self.feed_manager.download_from_url(
                url=feed_config['url'],
                headers=feed_config.get('headers'),
                auth=tuple(feed_config['auth']) if feed_config.get('auth') else None,
                timeout=feed_config.get('timeout', 30)
            )
            processor = FileProcessor()
            return processor.process_file_by_path(file_path)
            
        elif feed_type == 'google_sheets':
            return self.feed_manager.download_from_google_sheets(
                sheet_id=feed_config['sheet_id'],
                worksheet_name=feed_config.get('worksheet_name'),
                credentials_path=feed_config.get('credentials_path'),
                credentials_json=feed_config.get('credentials_json')
            )
            
        else:
            raise ValueError(f"Unsupported feed type: {feed_type}")
    
    def perform_batch_sync(self, shopify_client: ShopifyClient, sync_data: List[Dict], 
                          sync_fields: Dict = None, sync_options: Dict = None) -> List[Dict]:
        """Perform batch sync with selective field updates and enhanced error handling."""
        results = []
        
        # Use configured batch size or default
        batch_size = sync_options.get('batch_size', 5) if sync_options else 5
        sync_fields = sync_fields or {'inventory_quantity': True}  # Default to inventory only
        
        for i in range(0, len(sync_data), batch_size):
            batch = sync_data[i:i + batch_size]
            
            for item in batch:
                try:
                    # Skip zero inventory updates if configured
                    if (sync_options and sync_options.get('skip_zero_inventory') and 
                        item.get('new_quantity', 0) == 0):
                        continue
                    
                    # Prepare update data from the item
                    update_data = {
                        'quantity': item.get('new_quantity'),
                        'title': item.get('title'),
                        'price': item.get('price'),
                        'compare_at_price': item.get('compare_at_price'),
                        'vendor': item.get('vendor'),
                        'product_type': item.get('product_type'),
                        'sku': item.get('sku'),
                        'weight': item.get('weight')
                    }
                    
                    # Use new selective update method
                    if any(field for field in sync_fields.values() if field):
                        update_result = shopify_client.update_product_fields(
                            product_id=item.get('product_id'),
                            variant_id=item['variant_id'],
                            update_data=update_data,
                            sync_fields=sync_fields
                        )
                        results.append({
                            'variant_id': item['variant_id'],
                            'sku': item['shopify_sku'],
                            'success': True,
                            'error': None,
                            'updated_fields': [field for field, enabled in sync_fields.items() if enabled],
                            'details': update_result
                        })
                    else:
                        # Fallback to inventory-only update
                        shopify_client.update_inventory(
                            item['variant_id'],
                            item['new_quantity']
                        )
                        results.append({
                            'variant_id': item['variant_id'],
                            'sku': item['shopify_sku'],
                            'success': True,
                            'error': None,
                            'updated_fields': ['inventory_quantity']
                        })
                    
                except Exception as e:
                    error_msg = str(e)
                    self.logger.warning(f"Failed to sync SKU {item['shopify_sku']}: {error_msg}")
                    
                    # Classify error types for better handling
                    if "temporarily unavailable" in error_msg.lower():
                        # API overload - don't continue batch, fail fast
                        results.append({
                            'variant_id': item['variant_id'],
                            'sku': item['shopify_sku'],
                            'success': False,
                            'error': f"API overload: {error_msg}"
                        })
                        # Stop processing this batch to avoid more overload
                        self.logger.warning(f"API overload detected, stopping batch processing")
                        break
                    else:
                        results.append({
                            'variant_id': item['variant_id'],
                            'sku': item['shopify_sku'],
                            'success': False,
                            'error': error_msg
                        })
            
            # Small delay between batches to be respectful to API
            if i + batch_size < len(sync_data):
                time.sleep(1)
        
        return results
    
    def get_scheduled_jobs(self) -> List[Dict]:
        """Get list of all scheduled jobs."""
        jobs = []
        for job in self.scheduler.get_jobs():
            job_config = self.load_job_config(job.id)
            if job_config:
                job_info = {
                    'id': job.id,
                    'name': job.name,
                    'next_run': job.next_run_time,
                    'trigger': str(job.trigger),
                    **job_config
                }
                jobs.append(job_info)
        return jobs
    
    def get_job_history(self, job_id: str, limit: int = 50) -> List[Dict]:
        """Get execution history for a job."""
        history_file = f"job_history_{job_id}.json"
        if os.path.exists(history_file):
            try:
                with open(history_file, 'r') as f:
                    history = json.load(f)
                return history[-limit:]  # Return most recent executions
            except:
                return []
        return []
    
    def save_job_config(self, job_id: str, job_data: Dict):
        """Save job configuration to file."""
        config_file = f"job_config_{job_id}.json"
        try:
            with open(config_file, 'w') as f:
                json.dump(job_data, f, indent=2, default=str)
        except Exception as e:
            self.logger.error(f"Failed to save job config for '{job_id}': {str(e)}")
    
    def load_job_config(self, job_id: str) -> Optional[Dict]:
        """Load job configuration from file."""
        config_file = f"job_config_{job_id}.json"
        if os.path.exists(config_file):
            try:
                with open(config_file, 'r') as f:
                    return json.load(f)
            except:
                return None
        return None
    
    def delete_job_config(self, job_id: str):
        """Delete job configuration file."""
        config_file = f"job_config_{job_id}.json"
        if os.path.exists(config_file):
            try:
                os.remove(config_file)
            except Exception as e:
                self.logger.error(f"Failed to delete job config for '{job_id}': {str(e)}")
    
    def log_job_execution(self, result: Dict):
        """Log job execution result."""
        job_id = result['job_id']
        history_file = f"job_history_{job_id}.json"
        
        try:
            # Load existing history
            history = []
            if os.path.exists(history_file):
                with open(history_file, 'r') as f:
                    history = json.load(f)
            
            # Add new result
            history.append(result)
            
            # Keep only last 100 executions
            history = history[-100:]
            
            # Save updated history
            with open(history_file, 'w') as f:
                json.dump(history, f, indent=2, default=str)
                
        except Exception as e:
            self.logger.error(f"Failed to log job execution for '{job_id}': {str(e)}")
    
    def load_scheduled_jobs(self):
        """Load previously scheduled jobs from configuration files."""
        try:
            # Find all job config files
            for filename in os.listdir('.'):
                if filename.startswith('job_config_') and filename.endswith('.json'):
                    job_id = filename.replace('job_config_', '').replace('.json', '')
                    job_data = self.load_job_config(job_id)
                    
                    if job_data:
                        # Recreate the scheduled job
                        schedule_type = job_data.get('schedule_type')
                        schedule_config = job_data.get('schedule_config', {})
                        
                        if schedule_type == 'cron':
                            trigger = CronTrigger(**schedule_config)
                        elif schedule_type == 'interval':
                            trigger = IntervalTrigger(**schedule_config)
                        else:
                            continue
                        
                        self.scheduler.add_job(
                            func=self.execute_sync_job,
                            trigger=trigger,
                            args=[job_data],
                            id=job_id,
                            name=f"Sync Job: {job_id}",
                            replace_existing=True
                        )
                        
                        self.logger.info(f"Restored scheduled job '{job_id}'")
                        
        except Exception as e:
            self.logger.error(f"Failed to load scheduled jobs: {str(e)}")
    
    def job_executed(self, event):
        """Handle job execution event."""
        self.logger.info(f"Job '{event.job_id}' executed successfully")
    
    def job_error(self, event):
        """Handle job error event."""
        self.logger.error(f"Job '{event.job_id}' failed: {event.exception}")
    
    def shutdown(self):
        """Shutdown the scheduler."""
        self.scheduler.shutdown()
        self.logger.info("Scheduler shutdown complete")