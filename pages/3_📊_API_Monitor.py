import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import json
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.shopify_client import ShopifyClient
from src.scheduler import SyncScheduler
from utils.config import Config

st.set_page_config(
    page_title="API Monitor",
    page_icon="📊",
    layout="wide"
)

def main():
    st.title("📊 API Resilience Monitor")
    st.markdown("Monitor Shopify API health, resilience patterns, and performance metrics.")
    
    # Initialize components
    config = Config()
    
    if not config.validate_shopify_config():
        st.error("❌ Shopify configuration is missing. Please configure your API credentials first.")
        return
    
    # Tabs for different monitoring views
    tab1, tab2, tab3, tab4 = st.tabs(["🔍 Real-time Status", "📈 Performance Metrics", "🛡️ Resilience Patterns", "📋 Error Logs"])
    
    with tab1:
        real_time_status_tab()
    
    with tab2:
        performance_metrics_tab()
    
    with tab3:
        resilience_patterns_tab()
    
    with tab4:
        error_logs_tab()

def real_time_status_tab():
    st.header("🔍 Real-time API Status")
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        if st.button("🔄 Refresh Status", type="primary"):
            st.rerun()
    
    with col2:
        if st.button("🧪 Test API Connection"):
            test_api_connection()
    
    # Current API Status
    try:
        shopify_client = ShopifyClient()
        
        # Get API statistics
        api_stats = shopify_client.get_api_stats()
        
        # Display current status
        st.subheader("📊 Current API Statistics")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            circuit_state = api_stats.get('circuit_breaker_state', 'Unknown')
            if circuit_state == 'CLOSED':
                st.success(f"**Circuit Breaker**\n\n✅ {circuit_state}")
            elif circuit_state == 'OPEN':
                st.error(f"**Circuit Breaker**\n\n🚫 {circuit_state}")
            else:
                st.warning(f"**Circuit Breaker**\n\n⚠️ {circuit_state}")
        
        with col2:
            failure_count = api_stats.get('circuit_breaker_failures', 0)
            if failure_count == 0:
                st.success(f"**Failures**\n\n✅ {failure_count}")
            else:
                st.warning(f"**Failures**\n\n⚠️ {failure_count}")
        
        with col3:
            rate_limit_delay = api_stats.get('current_rate_limit_delay', 0)
            if rate_limit_delay <= 1.0:
                st.success(f"**Rate Limit Delay**\n\n✅ {rate_limit_delay:.2f}s")
            else:
                st.warning(f"**Rate Limit Delay**\n\n⚠️ {rate_limit_delay:.2f}s")
        
        with col4:
            success_count = api_stats.get('rate_limiter_success_count', 0)
            st.info(f"**Success Streak**\n\n📈 {success_count}")
        
        # Last request info
        last_request_time = api_stats.get('last_request_time', 0)
        if last_request_time > 0:
            last_request_dt = datetime.fromtimestamp(last_request_time)
            time_since = datetime.now() - last_request_dt
            st.info(f"🕐 **Last API Request**: {last_request_dt.strftime('%Y-%m-%d %H:%M:%S')} ({time_since} ago)")
        
        # Reset option
        if api_stats.get('circuit_breaker_state') == 'OPEN' or api_stats.get('current_rate_limit_delay', 0) > 5:
            if st.button("🔄 Reset API Resilience Patterns", type="secondary"):
                shopify_client.reset_resilience()
                st.success("✅ API resilience patterns have been reset")
                st.rerun()
        
        shopify_client.close()
        
    except Exception as e:
        st.error(f"❌ Error getting API status: {str(e)}")

def test_api_connection():
    """Test API connection with detailed feedback."""
    with st.spinner("Testing API connection..."):
        try:
            shopify_client = ShopifyClient()
            
            # Test basic connection
            start_time = datetime.now()
            is_connected = shopify_client.test_connection()
            end_time = datetime.now()
            
            response_time = (end_time - start_time).total_seconds()
            
            if is_connected:
                st.success(f"✅ API Connection successful! Response time: {response_time:.2f}s")
                
                # Get shop info for more details
                try:
                    shop_info = shopify_client.get_shop_info()
                    st.info(f"🏪 **Shop**: {shop_info.get('name', 'Unknown')} - {shop_info.get('domain', 'Unknown')}")
                except:
                    pass
            else:
                st.error("❌ API Connection failed!")
            
            shopify_client.close()
            
        except Exception as e:
            st.error(f"❌ Connection test failed: {str(e)}")

def performance_metrics_tab():
    st.header("📈 Performance Metrics")
    
    # Try to get scheduler for job performance data
    try:
        if 'scheduler' not in st.session_state:
            st.session_state.scheduler = SyncScheduler()
        
        jobs = st.session_state.scheduler.get_scheduled_jobs()
        
        if jobs:
            # Create performance overview
            st.subheader("🎯 Job Performance Overview")
            
            # Calculate metrics
            total_jobs = len(jobs)
            active_jobs = len([j for j in jobs if j.get('next_run')])
            total_runs = sum(j.get('run_count', 0) for j in jobs)
            total_successes = sum(j.get('success_count', 0) for j in jobs)
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Jobs", total_jobs)
            with col2:
                st.metric("Active Jobs", active_jobs)
            with col3:
                st.metric("Total Executions", total_runs)
            with col4:
                success_rate = (total_successes / total_runs * 100) if total_runs > 0 else 0
                st.metric("Overall Success Rate", f"{success_rate:.1f}%")
            
            # Job performance chart
            if jobs:
                st.subheader("📊 Job Success Rates")
                
                job_data = []
                for job in jobs:
                    run_count = job.get('run_count', 0)
                    success_count = job.get('success_count', 0)
                    success_rate = (success_count / run_count * 100) if run_count > 0 else 0
                    
                    job_data.append({
                        'Job ID': job['id'],
                        'Runs': run_count,
                        'Successes': success_count,
                        'Success Rate': success_rate
                    })
                
                if job_data:
                    df = pd.DataFrame(job_data)
                    
                    fig = px.bar(df, x='Job ID', y='Success Rate', 
                                title="Job Success Rates",
                                color='Success Rate',
                                color_continuous_scale=['red', 'yellow', 'green'])
                    
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Performance table
                    st.dataframe(df, use_container_width=True)
        else:
            st.info("📭 No scheduled jobs found to analyze.")
    
    except Exception as e:
        st.error(f"❌ Error loading performance metrics: {str(e)}")

def resilience_patterns_tab():
    st.header("🛡️ Resilience Patterns Analysis")
    
    st.markdown("""
    This tab shows how the API resilience patterns are protecting your application from failures.
    """)
    
    # Get current API client stats
    try:
        shopify_client = ShopifyClient()
        api_stats = shopify_client.get_api_stats()
        
        # Circuit Breaker Analysis
        st.subheader("⚡ Circuit Breaker Status")
        
        col1, col2 = st.columns(2)
        
        with col1:
            state = api_stats.get('circuit_breaker_state', 'UNKNOWN')
            failures = api_stats.get('circuit_breaker_failures', 0)
            
            if state == 'CLOSED':
                st.success("✅ **Circuit Breaker: CLOSED**\n\nAPI calls are flowing normally")
            elif state == 'OPEN':
                st.error("🚫 **Circuit Breaker: OPEN**\n\nAPI calls are being blocked due to failures")
            elif state == 'HALF_OPEN':
                st.warning("⚠️ **Circuit Breaker: HALF-OPEN**\n\nTesting if API has recovered")
            else:
                st.info("❓ **Circuit Breaker: UNKNOWN**\n\nState information not available")
            
            st.metric("Current Failure Count", failures)
        
        with col2:
            # Circuit breaker configuration
            st.info("🔧 **Circuit Breaker Config**")
            st.write("- **Failure Threshold**: 3 failures")
            st.write("- **Recovery Timeout**: 60 seconds")
            st.write("- **Half-Open Testing**: Automatic")
        
        # Rate Limiter Analysis
        st.subheader("🚦 Adaptive Rate Limiter")
        
        col1, col2 = st.columns(2)
        
        with col1:
            current_delay = api_stats.get('current_rate_limit_delay', 0.5)
            success_streak = api_stats.get('rate_limiter_success_count', 0)
            
            st.metric("Current Delay", f"{current_delay:.2f}s")
            st.metric("Success Streak", success_streak)
            
            # Delay status
            if current_delay <= 1.0:
                st.success("✅ **Rate Limiting**: Normal speed")
            elif current_delay <= 5.0:
                st.warning("⚠️ **Rate Limiting**: Moderate throttling")
            else:
                st.error("🚫 **Rate Limiting**: Heavy throttling")
        
        with col2:
            st.info("🔧 **Rate Limiter Config**")
            st.write("- **Initial Delay**: 0.5 seconds")
            st.write("- **Maximum Delay**: 30.0 seconds")
            st.write("- **Adaptive**: Yes")
            st.write("- **Auto-Recovery**: After 5 successes")
        
        # Resilience Timeline
        st.subheader("📈 Resilience Effectiveness")
        
        # Create a mock timeline showing how resilience patterns help
        timeline_data = [
            {"Time": "Normal Operation", "Delay": 0.5, "State": "CLOSED", "Status": "Healthy"},
            {"Time": "Rate Limited", "Delay": 2.0, "State": "CLOSED", "Status": "Throttled"},
            {"Time": "API Overload", "Delay": 10.0, "State": "CLOSED", "Status": "Heavy Throttling"},
            {"Time": "Multiple Failures", "Delay": 30.0, "State": "OPEN", "Status": "Protected"},
            {"Time": "Recovery Test", "Delay": 15.0, "State": "HALF_OPEN", "Status": "Testing"},
            {"Time": "Recovered", "Delay": 1.0, "State": "CLOSED", "Status": "Recovering"}
        ]
        
        df_timeline = pd.DataFrame(timeline_data)
        
        fig = go.Figure()
        
        # Add delay line
        fig.add_trace(go.Scatter(
            x=df_timeline['Time'],
            y=df_timeline['Delay'],
            mode='lines+markers',
            name='Request Delay (s)',
            line=dict(color='blue')
        ))
        
        fig.update_layout(
            title="API Resilience Pattern Response",
            xaxis_title="Scenario",
            yaxis_title="Request Delay (seconds)",
            height=400
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        shopify_client.close()
        
    except Exception as e:
        st.error(f"❌ Error analyzing resilience patterns: {str(e)}")

def error_logs_tab():
    st.header("📋 Error Logs & Analysis")
    
    st.markdown("""
    View and analyze API errors to understand patterns and improve reliability.
    """)
    
    # Try to load error logs from scheduler
    try:
        if 'scheduler' not in st.session_state:
            st.session_state.scheduler = SyncScheduler()
        
        jobs = st.session_state.scheduler.get_scheduled_jobs()
        
        if jobs:
            # Collect all errors from job history
            all_errors = []
            
            for job in jobs:
                history = st.session_state.scheduler.get_job_history(job['id'], limit=50)
                
                for execution in history:
                    if not execution.get('success', True) and execution.get('error'):
                        all_errors.append({
                            'timestamp': execution.get('start_time', 'Unknown'),
                            'job_id': job['id'],
                            'error': execution.get('error', ''),
                            'duration': execution.get('duration', 0)
                        })
            
            if all_errors:
                st.subheader(f"🚨 Recent Errors ({len(all_errors)} found)")
                
                # Error categorization
                api_overload_errors = [e for e in all_errors if 'overload' in e['error'].lower() or '529' in e['error']]
                rate_limit_errors = [e for e in all_errors if 'rate limit' in e['error'].lower() or '429' in e['error']]
                connection_errors = [e for e in all_errors if 'connection' in e['error'].lower() or 'timeout' in e['error'].lower()]
                other_errors = [e for e in all_errors if e not in api_overload_errors + rate_limit_errors + connection_errors]
                
                # Error summary
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("API Overload (529)", len(api_overload_errors))
                with col2:
                    st.metric("Rate Limited (429)", len(rate_limit_errors))
                with col3:
                    st.metric("Connection Issues", len(connection_errors))
                with col4:
                    st.metric("Other Errors", len(other_errors))
                
                # Error timeline
                if len(all_errors) > 1:
                    st.subheader("📈 Error Timeline")
                    
                    error_df = pd.DataFrame(all_errors)
                    error_df['timestamp'] = pd.to_datetime(error_df['timestamp'])
                    error_df['date'] = error_df['timestamp'].dt.date
                    
                    daily_errors = error_df.groupby('date').size().reset_index(name='error_count')
                    daily_errors['date'] = pd.to_datetime(daily_errors['date'])
                    
                    fig = px.line(daily_errors, x='date', y='error_count',
                                 title="Daily Error Count",
                                 markers=True)
                    
                    fig.update_layout(height=300)
                    st.plotly_chart(fig, use_container_width=True)
                
                # Recent errors table
                st.subheader("📝 Recent Error Details")
                
                # Show last 10 errors
                recent_errors = sorted(all_errors, key=lambda x: x['timestamp'], reverse=True)[:10]
                
                for error in recent_errors:
                    with st.expander(f"🚨 {error['job_id']} - {error['timestamp']}"):
                        
                        error_text = error['error']
                        
                        # Color code based on error type
                        if 'overload' in error_text.lower() or '529' in error_text:
                            st.error(f"**API Overload Error**: {error_text}")
                        elif 'rate limit' in error_text.lower() or '429' in error_text:
                            st.warning(f"**Rate Limit Error**: {error_text}")
                        elif 'connection' in error_text.lower():
                            st.info(f"**Connection Error**: {error_text}")
                        else:
                            st.error(f"**Error**: {error_text}")
                        
                        if error['duration'] > 0:
                            st.write(f"**Duration**: {error['duration']:.2f} seconds")
            else:
                st.success("✅ No recent errors found!")
        else:
            st.info("📭 No scheduled jobs found to analyze errors.")
    
    except Exception as e:
        st.error(f"❌ Error loading error logs: {str(e)}")
    
    # Error pattern analysis
    st.subheader("🔍 Error Pattern Analysis")
    
    with st.expander("📖 Understanding API Errors"):
        st.markdown("""
        **Common API Error Patterns:**
        
        - **529 Overloaded**: API is receiving too many requests globally
        - **429 Rate Limited**: Your app is making requests too quickly  
        - **Connection Timeouts**: Network or server issues
        - **5xx Server Errors**: Shopify server problems
        
        **How Resilience Patterns Help:**
        
        - **Exponential Backoff**: Automatically retries with increasing delays
        - **Circuit Breaker**: Stops requests when API is consistently failing
        - **Adaptive Rate Limiting**: Adjusts request speed based on API responses
        - **Jitter**: Adds randomness to prevent thundering herd problems
        """)

if __name__ == "__main__":
    main()