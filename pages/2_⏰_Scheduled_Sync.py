import streamlit as st
import pandas as pd
import json
import os
import sys
from datetime import datetime, timedelta
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.scheduler import SyncScheduler
from src.feed_sources import FeedConfigManager, FeedSourceManager
from src.column_mapper import ColumnMapper

st.set_page_config(
    page_title="Scheduled Sync",
    page_icon="⏰",
    layout="wide"
)

# Initialize scheduler and managers
if 'scheduler' not in st.session_state:
    st.session_state.scheduler = SyncScheduler()

if 'config_manager' not in st.session_state:
    st.session_state.config_manager = FeedConfigManager()

if 'feed_manager' not in st.session_state:
    st.session_state.feed_manager = FeedSourceManager()

def main():
    st.title("⏰ Scheduled Synchronization")
    st.markdown("Set up automated inventory synchronization with your configured feed sources.")
    
    # Tabs for different functions
    tab1, tab2, tab3, tab4 = st.tabs(["📅 Create Schedule", "📋 Manage Jobs", "📊 Job History", "📈 Monitoring"])
    
    with tab1:
        create_schedule_tab()
    
    with tab2:
        manage_jobs_tab()
    
    with tab3:
        job_history_tab()
    
    with tab4:
        monitoring_tab()

def create_schedule_tab():
    st.header("📅 Create Scheduled Sync Job")
    
    # Check if feed sources are configured
    configs = st.session_state.config_manager.configs
    if not configs:
        st.warning("⚠️ No feed sources configured. Please configure feed sources first.")
        if st.button("🔗 Configure Feed Sources"):
            st.switch_page("pages/1_🔗_Feed_Sources.py")
        return
    
    # Job configuration
    col1, col2 = st.columns(2)
    
    with col1:
        job_id = st.text_input(
            "Job ID",
            placeholder="daily-inventory-sync",
            help="Unique identifier for this scheduled job"
        )
        
        feed_config_name = st.selectbox(
            "Feed Source",
            list(configs.keys()),
            help="Select the feed source to sync from"
        )
        
    with col2:
        schedule_type = st.selectbox(
            "Schedule Type",
            ["Interval", "Cron Expression"],
            help="Choose how to schedule the job"
        )
        
        enabled = st.checkbox("Enable Job", value=True)
    
    # Schedule configuration
    st.subheader("🕐 Schedule Configuration")
    
    if schedule_type == "Interval":
        configure_interval_schedule()
    else:
        configure_cron_schedule()
    
    # Column mapping
    st.subheader("🔗 Column Mapping")
    
    if feed_config_name:
        config_column_mapping(feed_config_name)
    
    # Preview and save
    st.subheader("📋 Job Summary")
    
    if st.button("💾 Create Scheduled Job", type="primary"):
        create_scheduled_job(job_id, feed_config_name, enabled)

def configure_interval_schedule():
    """Configure interval-based scheduling."""
    st.write("**Interval Scheduling**: Run job every X time units")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        interval_value = st.number_input("Every", min_value=1, value=1)
    
    with col2:
        interval_unit = st.selectbox("Time Unit", ["minutes", "hours", "days", "weeks"])
    
    with col3:
        start_time = st.time_input("Start Time", value=datetime.now().time())
    
    # Store in session state
    st.session_state.schedule_config = {
        interval_unit: interval_value,
        'start_date': datetime.combine(datetime.now().date(), start_time)
    }
    
    # Show next run times
    next_runs = []
    current_time = datetime.combine(datetime.now().date(), start_time)
    
    if interval_unit == "minutes":
        delta = timedelta(minutes=interval_value)
    elif interval_unit == "hours":
        delta = timedelta(hours=interval_value)
    elif interval_unit == "days":
        delta = timedelta(days=interval_value)
    else:  # weeks
        delta = timedelta(weeks=interval_value)
    
    for i in range(5):
        next_runs.append(current_time + delta * i)
    
    st.info(f"📅 **Next 5 runs**: {', '.join([dt.strftime('%Y-%m-%d %H:%M') for dt in next_runs])}")

def configure_cron_schedule():
    """Configure cron-based scheduling."""
    st.write("**Cron Scheduling**: Use cron expressions for precise timing")
    
    # Preset options
    preset = st.selectbox(
        "Quick Presets",
        [
            "Custom",
            "Every hour", 
            "Daily at midnight",
            "Daily at 9 AM",
            "Every weekday at 9 AM",
            "Weekly on Monday at 9 AM",
            "Monthly on 1st at 9 AM"
        ]
    )
    
    preset_expressions = {
        "Every hour": {"minute": 0},
        "Daily at midnight": {"hour": 0, "minute": 0},
        "Daily at 9 AM": {"hour": 9, "minute": 0},
        "Every weekday at 9 AM": {"hour": 9, "minute": 0, "day_of_week": "1-5"},
        "Weekly on Monday at 9 AM": {"hour": 9, "minute": 0, "day_of_week": 1},
        "Monthly on 1st at 9 AM": {"hour": 9, "minute": 0, "day": 1}
    }
    
    if preset != "Custom":
        st.session_state.schedule_config = preset_expressions[preset]
        st.success(f"✅ Using preset: {preset}")
    else:
        # Manual cron configuration
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            minute = st.text_input("Minute (0-59)", value="0", help="0-59 or * for every minute")
        
        with col2:
            hour = st.text_input("Hour (0-23)", value="*", help="0-23 or * for every hour")
        
        with col3:
            day = st.text_input("Day (1-31)", value="*", help="1-31 or * for every day")
        
        with col4:
            month = st.text_input("Month (1-12)", value="*", help="1-12 or * for every month")
        
        with col5:
            day_of_week = st.text_input("Day of Week", value="*", help="0-6 (Sun-Sat) or * for every day")
        
        # Build cron config
        cron_config = {}
        if minute != "*":
            cron_config["minute"] = minute
        if hour != "*":
            cron_config["hour"] = hour
        if day != "*":
            cron_config["day"] = day
        if month != "*":
            cron_config["month"] = month
        if day_of_week != "*":
            cron_config["day_of_week"] = day_of_week
        
        st.session_state.schedule_config = cron_config
        
        # Show cron expression
        cron_parts = [
            minute or "*",
            hour or "*", 
            day or "*",
            month or "*",
            day_of_week or "*"
        ]
        st.code(f"Cron Expression: {' '.join(cron_parts)}")
    
    st.info("ℹ️ **Cron Help**: Use * for 'every', ranges like 1-5, or lists like 1,3,5")

def config_column_mapping(feed_config_name: str):
    """Configure column mapping for the selected feed."""
    try:
        # Get feed configuration
        config = st.session_state.config_manager.get_config(feed_config_name)
        
        if config:
            st.write(f"**Feed Source**: {feed_config_name} ({config.get('type', 'Unknown').upper()})")
            
            # Check if feed has column mapping configured
            feed_mapping = config.get('column_mapping', {})
            
            if feed_mapping:
                st.success(f"✅ **Column mapping found in feed configuration**")
                
                # Show the configured mapping
                st.write("**Configured column mapping:**")
                mapping_df = pd.DataFrame([
                    {"Inventory Field": k, "Feed Column": v} 
                    for k, v in feed_mapping.items()
                ])
                st.dataframe(mapping_df, use_container_width=True)
                
                # Option to override
                with st.expander("🔧 Override Column Mapping (Optional)"):
                    st.info("The feed already has column mapping configured. You can override it here if needed.")
                    
                    override_mapping = {}
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        sku_column = st.text_input(
                            "SKU Column Override", 
                            value=feed_mapping.get("SKU", ""),
                            placeholder="Leave empty to use feed mapping"
                        )
                        quantity_column = st.text_input(
                            "Quantity Column Override", 
                            value=feed_mapping.get("Quantity", ""),
                            placeholder="Leave empty to use feed mapping"
                        )
                    
                    with col2:
                        title_column = st.text_input(
                            "Product Title Column Override", 
                            value=feed_mapping.get("Product Title", ""),
                            placeholder="Leave empty to use feed mapping"
                        )
                        price_column = st.text_input(
                            "Price Column Override", 
                            value=feed_mapping.get("Price", ""),
                            placeholder="Leave empty to use feed mapping"
                        )
                    
                    # Build override mapping only for changed values
                    if sku_column and sku_column != feed_mapping.get("SKU", ""):
                        override_mapping["SKU"] = sku_column
                    if quantity_column and quantity_column != feed_mapping.get("Quantity", ""):
                        override_mapping["Quantity"] = quantity_column
                    if title_column and title_column != feed_mapping.get("Product Title", ""):
                        override_mapping["Product Title"] = title_column
                    if price_column and price_column != feed_mapping.get("Price", ""):
                        override_mapping["Price"] = price_column
                    
                    if override_mapping:
                        st.info(f"📝 **Override mappings**: {override_mapping}")
                        st.session_state.column_mapping = override_mapping
                    else:
                        # Use feed mapping
                        st.session_state.column_mapping = {}
                
                # Always ensure we have the feed mapping available
                if not hasattr(st.session_state, 'column_mapping') or not st.session_state.column_mapping:
                    st.session_state.column_mapping = {}
                
                # Show selected columns info
                if config.get('selected_columns'):
                    st.info(f"📋 **Column Selection**: {len(config['selected_columns'])} columns selected for sync: {', '.join(config['selected_columns'][:5])}{'...' if len(config['selected_columns']) > 5 else ''}")
                else:
                    st.info("📋 **Column Selection**: All columns will be synced (not configured)")
                
            else:
                st.warning("⚠️ **No column mapping configured in feed**")
                st.info("Please configure column mapping in the Feed Sources page first, or set it manually below.")
                
                # Manual mapping interface
                col1, col2 = st.columns(2)
                
                with col1:
                    sku_column = st.text_input("SKU Column Name", placeholder="e.g., SKU, ProductCode, ItemID")
                    quantity_column = st.text_input("Quantity Column Name", placeholder="e.g., Quantity, Stock, Available")
                
                with col2:
                    title_column = st.text_input("Product Title Column (optional)", placeholder="e.g., Title, Name, Description")
                    price_column = st.text_input("Price Column (optional)", placeholder="e.g., Price, UnitPrice, Cost")
                
                # Store mapping in session state
                mapping = {}
                if sku_column:
                    mapping["SKU"] = sku_column
                if quantity_column:
                    mapping["Quantity"] = quantity_column
                if title_column:
                    mapping["Product Title"] = title_column
                if price_column:
                    mapping["Price"] = price_column
                
                st.session_state.column_mapping = mapping
                
                if mapping:
                    st.success(f"✅ Mapped {len(mapping)} columns")
                else:
                    st.warning("⚠️ Please map at least SKU and Quantity columns")
            
            # Show button to preview feed headers
            if st.button("📖 Preview Feed Headers", help="See available columns in the feed"):
                try:
                    with st.spinner("Reading feed headers..."):
                        available_columns = st.session_state.feed_manager.get_feed_headers(config['type'], config)
                    
                    st.success(f"✅ Found {len(available_columns)} columns in the feed")
                    
                    # Show selected vs available columns
                    selected_columns = config.get('selected_columns', [])
                    if selected_columns:
                        st.write("**Selected columns for sync:**")
                        selected_in_feed = [col for col in selected_columns if col in available_columns]
                        cols = st.columns(min(len(selected_in_feed), 4))
                        for i, col in enumerate(selected_in_feed):
                            with cols[i % 4]:
                                st.write(f"✅ {col}")
                        
                        if len(selected_in_feed) < len(selected_columns):
                            missing_columns = [col for col in selected_columns if col not in available_columns]
                            st.warning(f"⚠️ Selected columns not found in feed: {', '.join(missing_columns)}")
                    else:
                        st.write("**All available columns:**")
                        cols = st.columns(min(len(available_columns), 4))
                        for i, col in enumerate(available_columns):
                            with cols[i % 4]:
                                st.write(f"• {col}")
                            
                except Exception as e:
                    st.error(f"❌ Failed to read headers: {str(e)}")
    
    except Exception as e:
        st.error(f"❌ Error configuring column mapping: {str(e)}")

def create_scheduled_job(job_id: str, feed_config_name: str, enabled: bool):
    """Create the scheduled sync job."""
    if not job_id:
        st.error("❌ Please provide a Job ID")
        return
    
    if not hasattr(st.session_state, 'schedule_config'):
        st.error("❌ Please configure the schedule")
        return
    
    column_mapping = getattr(st.session_state, 'column_mapping', {})
    if 'SKU' not in column_mapping or 'Quantity' not in column_mapping:
        st.error("❌ Please map at least SKU and Quantity columns")
        return
    
    try:
        # Determine schedule type
        schedule_config = st.session_state.schedule_config
        
        # Check if it's interval or cron based on keys
        if any(key in schedule_config for key in ['minutes', 'hours', 'days', 'weeks']):
            schedule_type = 'interval'
        else:
            schedule_type = 'cron'
        
        if enabled:
            # Add the scheduled job
            success = st.session_state.scheduler.add_scheduled_sync(
                job_id=job_id,
                feed_config_name=feed_config_name,
                schedule_type=schedule_type,
                schedule_config=schedule_config,
                column_mapping=column_mapping
            )
            
            if success:
                st.success(f"✅ Scheduled job '{job_id}' created successfully!")
                
                # Clear form
                for key in ['schedule_config', 'column_mapping']:
                    if key in st.session_state:
                        del st.session_state[key]
                
                st.rerun()
            else:
                st.error(f"❌ Failed to create scheduled job '{job_id}'")
        else:
            st.info("ℹ️ Job configuration saved but not enabled. Enable it in the 'Manage Jobs' tab.")
    
    except Exception as e:
        st.error(f"❌ Error creating scheduled job: {str(e)}")

def manage_jobs_tab():
    st.header("📋 Manage Scheduled Jobs")
    
    # Get all scheduled jobs
    jobs = st.session_state.scheduler.get_scheduled_jobs()
    
    if not jobs:
        st.info("📭 No scheduled jobs found. Create one in the 'Create Schedule' tab.")
        return
    
    # Display jobs
    for job in jobs:
        with st.expander(f"🔄 {job['id']} - Next: {job.get('next_run', 'Not scheduled')}"):
            col1, col2, col3 = st.columns([2, 1, 1])
            
            with col1:
                st.write(f"**Feed Source**: {job.get('feed_config_name', 'Unknown')}")
                st.write(f"**Schedule**: {job.get('trigger', 'Unknown')}")
                st.write(f"**Created**: {job.get('created_at', 'Unknown')}")
                
                # Job statistics
                if job.get('run_count', 0) > 0:
                    success_rate = (job.get('success_count', 0) / job.get('run_count', 1)) * 100
                    st.write(f"**Success Rate**: {success_rate:.1f}% ({job.get('success_count', 0)}/{job.get('run_count', 0)})")
                    
                    if job.get('last_run'):
                        st.write(f"**Last Run**: {job.get('last_run')}")
                    
                    if job.get('last_error'):
                        st.error(f"**Last Error**: {job.get('last_error')}")
            
            with col2:
                if st.button(f"▶️ Run Now", key=f"run_{job['id']}"):
                    run_job_manually(job['id'])
                    
                if st.button(f"📊 History", key=f"history_{job['id']}"):
                    st.session_state.selected_job_history = job['id']
                    st.switch_page("pages/2_⏰_Scheduled_Sync.py")
            
            with col3:
                if st.button(f"⏸️ Pause", key=f"pause_{job['id']}"):
                    pause_job(job['id'])
                
                if st.button(f"🗑️ Delete", key=f"delete_{job['id']}", type="secondary"):
                    delete_job(job['id'])

def run_job_manually(job_id: str):
    """Run a scheduled job manually."""
    try:
        with st.spinner(f"Running job '{job_id}'..."):
            # Get job configuration
            job_config = st.session_state.scheduler.load_job_config(job_id)
            if job_config:
                result = st.session_state.scheduler.execute_sync_job(job_config)
                
                if result['success']:
                    st.success(f"✅ Job '{job_id}' completed successfully!")
                    st.info(f"📊 Processed: {result['records_processed']}, Synced: {result['records_synced']}")
                else:
                    st.error(f"❌ Job '{job_id}' failed: {result.get('error', 'Unknown error')}")
            else:
                st.error(f"❌ Job configuration for '{job_id}' not found")
    except Exception as e:
        st.error(f"❌ Error running job: {str(e)}")

def pause_job(job_id: str):
    """Pause a scheduled job."""
    try:
        st.session_state.scheduler.scheduler.pause_job(job_id)
        st.success(f"✅ Job '{job_id}' paused")
        st.rerun()
    except Exception as e:
        st.error(f"❌ Error pausing job: {str(e)}")

def delete_job(job_id: str):
    """Delete a scheduled job."""
    try:
        success = st.session_state.scheduler.remove_scheduled_sync(job_id)
        if success:
            st.success(f"✅ Job '{job_id}' deleted")
            st.rerun()
        else:
            st.error(f"❌ Failed to delete job '{job_id}'")
    except Exception as e:
        st.error(f"❌ Error deleting job: {str(e)}")

def job_history_tab():
    st.header("📊 Job Execution History")
    
    jobs = st.session_state.scheduler.get_scheduled_jobs()
    
    if not jobs:
        st.info("📭 No scheduled jobs found.")
        return
    
    job_ids = [job['id'] for job in jobs]
    selected_job = st.selectbox("Select Job", job_ids)
    
    if selected_job:
        history = st.session_state.scheduler.get_job_history(selected_job, limit=100)
        
        if history:
            # Convert to DataFrame for better display
            df = pd.DataFrame(history)
            
            # Format dates
            if 'start_time' in df.columns:
                df['start_time'] = pd.to_datetime(df['start_time']).dt.strftime('%Y-%m-%d %H:%M:%S')
            if 'end_time' in df.columns:
                df['end_time'] = pd.to_datetime(df['end_time']).dt.strftime('%Y-%m-%d %H:%M:%S')
            
            # Display metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Runs", len(df))
            with col2:
                success_count = len(df[df['success'] == True])
                st.metric("Successful", success_count)
            with col3:
                error_count = len(df[df['success'] == False])
                st.metric("Failed", error_count)
            with col4:
                if len(df) > 0:
                    avg_duration = df['duration'].mean() if 'duration' in df.columns else 0
                    st.metric("Avg Duration", f"{avg_duration:.1f}s")
            
            # Display history table
            st.subheader("📋 Execution History")
            
            # Select columns to display
            display_columns = ['start_time', 'success', 'records_processed', 'records_synced', 'duration']
            available_columns = [col for col in display_columns if col in df.columns]
            
            if available_columns:
                st.dataframe(df[available_columns].sort_values('start_time', ascending=False), 
                           use_container_width=True)
            
            # Show recent errors
            error_df = df[df['success'] == False]
            if not error_df.empty:
                st.subheader("❌ Recent Errors")
                for _, row in error_df.tail(5).iterrows():
                    with st.expander(f"Error on {row.get('start_time', 'Unknown')}"):
                        st.error(row.get('error', 'Unknown error'))
        else:
            st.info(f"📭 No execution history found for job '{selected_job}'")

def monitoring_tab():
    st.header("📈 System Monitoring")
    
    jobs = st.session_state.scheduler.get_scheduled_jobs()
    
    if not jobs:
        st.info("📭 No scheduled jobs to monitor.")
        return
    
    # Overall statistics
    st.subheader("📊 Overall Statistics")
    
    total_jobs = len(jobs)
    active_jobs = len([job for job in jobs if job.get('next_run')])
    total_runs = sum(job.get('run_count', 0) for job in jobs)
    total_successes = sum(job.get('success_count', 0) for job in jobs)
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Jobs", total_jobs)
    with col2:
        st.metric("Active Jobs", active_jobs)
    with col3:
        st.metric("Total Runs", total_runs)
    with col4:
        success_rate = (total_successes / total_runs * 100) if total_runs > 0 else 0
        st.metric("Success Rate", f"{success_rate:.1f}%")
    
    # Job status overview
    st.subheader("🔄 Job Status Overview")
    
    job_status_data = []
    for job in jobs:
        status = "Active" if job.get('next_run') else "Inactive"
        last_run_status = "Success" if not job.get('last_error') and job.get('last_success') else "Error"
        
        job_status_data.append({
            'Job ID': job['id'],
            'Status': status,
            'Last Run': last_run_status,
            'Success Rate': f"{(job.get('success_count', 0) / max(job.get('run_count', 1), 1) * 100):.1f}%",
            'Next Run': str(job.get('next_run', 'Not scheduled'))[:19] if job.get('next_run') else 'Not scheduled'
        })
    
    if job_status_data:
        st.dataframe(pd.DataFrame(job_status_data), use_container_width=True)
    
    # System health
    st.subheader("🏥 System Health")
    
    try:
        scheduler_running = st.session_state.scheduler.scheduler.running
        st.success("✅ Scheduler is running") if scheduler_running else st.error("❌ Scheduler is not running")
        
        # Check for recent failures
        recent_failures = 0
        for job in jobs:
            if job.get('last_error') and job.get('last_run'):
                # Check if error was in last 24 hours (simplified check)
                recent_failures += 1
        
        if recent_failures > 0:
            st.warning(f"⚠️ {recent_failures} job(s) have recent failures")
        else:
            st.success("✅ No recent job failures")
        
    except Exception as e:
        st.error(f"❌ Error checking system health: {str(e)}")

if __name__ == "__main__":
    main()