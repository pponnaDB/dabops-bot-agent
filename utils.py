"""
Utility Functions Module
Common utility functions for the DABOps application.
"""

import os
import logging
import functools
import streamlit as st
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Callable
from databricks.sdk.core import DatabricksError

def setup_logging():
    """Set up logging configuration for the application."""
    log_level = os.getenv('DABOPS_LOG_LEVEL', 'INFO').upper()
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format=log_format,
        handlers=[
            logging.StreamHandler(),
            # Add file handler if log file is specified
        ]
    )
    
    # Set specific loggers
    logging.getLogger('databricks').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    
    logger = logging.getLogger(__name__)
    logger.info(f"Logging initialized at {log_level} level")

def handle_error(error: Exception, message: str = "An error occurred", show_details: bool = True):
    """
    Handle and display errors in the Streamlit app.
    
    Args:
        error: The exception that occurred
        message: Custom error message to display
        show_details: Whether to show error details
    """
    logger = logging.getLogger(__name__)
    logger.error(f"{message}: {str(error)}")
    
    # Display error in Streamlit
    st.error(message)
    
    if show_details:
        with st.expander("🔍 Error Details"):
            st.code(f"""
Error Type: {type(error).__name__}
Error Message: {str(error)}
Timestamp: {datetime.now().isoformat()}
""")

def handle_databricks_error(func: Callable) -> Callable:
    """
    Decorator to handle Databricks API errors gracefully.
    
    Args:
        func: Function to decorate
        
    Returns:
        Decorated function with error handling
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except DatabricksError as e:
            logger = logging.getLogger(__name__)
            logger.error(f"Databricks API error in {func.__name__}: {str(e)}")
            
            # Parse Databricks error for better user messages
            error_message = str(e)
            if "PERMISSION_DENIED" in error_message:
                st.error("❌ Permission denied. Please check your Databricks access rights.")
            elif "UNAUTHENTICATED" in error_message:
                st.error("❌ Authentication failed. Please check your Databricks credentials.")
            elif "NOT_FOUND" in error_message:
                st.error("❌ Resource not found. The requested item may have been deleted.")
            elif "QUOTA_EXCEEDED" in error_message:
                st.error("❌ Quota exceeded. Please contact your workspace administrator.")
            else:
                st.error(f"❌ Databricks API error: {error_message}")
            
            return None
        except Exception as e:
            handle_error(e, f"Error in {func.__name__}")
            return None
    
    return wrapper

def format_job_info(timestamp: Optional[int]) -> str:
    """
    Format job timestamp for display.
    
    Args:
        timestamp: Unix timestamp in milliseconds
        
    Returns:
        Formatted datetime string
    """
    if not timestamp:
        return "Unknown"
    
    try:
        # Convert from milliseconds to seconds
        dt = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    except (ValueError, OSError):
        return "Invalid date"

def format_file_size(size_bytes: int) -> str:
    """
    Format file size in human-readable format.
    
    Args:
        size_bytes: Size in bytes
        
    Returns:
        Formatted size string
    """
    if size_bytes == 0:
        return "0 B"
    
    size_names = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    while size_bytes >= 1024 and i < len(size_names) - 1:
        size_bytes /= 1024.0
        i += 1
    
    return f"{size_bytes:.1f} {size_names[i]}"

def validate_bundle_name(name: str) -> bool:
    """
    Validate bundle name format.
    
    Args:
        name: Bundle name to validate
        
    Returns:
        True if valid, False otherwise
    """
    if not name or len(name.strip()) == 0:
        return False
    
    # Check for valid characters (alphanumeric, underscore, hyphen)
    import re
    if not re.match(r'^[a-zA-Z0-9_-]+$', name.strip()):
        return False
    
    # Check length limits
    if len(name.strip()) > 100:
        return False
    
    return True

def sanitize_filename(filename: str) -> str:
    """
    Sanitize filename for safe filesystem usage.
    
    Args:
        filename: Original filename
        
    Returns:
        Sanitized filename
    """
    import re
    
    # Remove or replace invalid characters
    filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
    
    # Remove leading/trailing whitespace and dots
    filename = filename.strip(' .')
    
    # Limit length
    if len(filename) > 255:
        name, ext = os.path.splitext(filename)
        filename = name[:255-len(ext)] + ext
    
    # Ensure it's not empty
    if not filename:
        filename = "untitled"
    
    return filename

def create_workspace_path(base_path: str, user: str, *path_components: str) -> str:
    """
    Create a workspace path with proper formatting.
    
    Args:
        base_path: Base path template
        user: Username
        *path_components: Additional path components
        
    Returns:
        Properly formatted workspace path
    """
    # Replace user placeholder
    path = base_path.format(user=user)
    
    # Add additional components
    for component in path_components:
        path = os.path.join(path, component).replace('\\', '/')
    
    # Ensure it starts with /Workspace
    if not path.startswith('/Workspace'):
        path = '/Workspace' + path
    
    return path

def parse_cron_expression(cron_expr: str) -> Dict[str, Any]:
    """
    Parse and validate cron expression.
    
    Args:
        cron_expr: Quartz cron expression
        
    Returns:
        Dictionary with parsed components
    """
    try:
        # Basic parsing for Quartz cron (7 fields: seconds, minutes, hours, day, month, weekday, year)
        parts = cron_expr.split()
        
        if len(parts) not in [6, 7]:  # Unix cron (6 fields) or Quartz cron (7 fields)
            raise ValueError("Invalid cron expression format")
        
        fields = ['seconds', 'minutes', 'hours', 'day', 'month', 'weekday']
        if len(parts) == 7:
            fields.append('year')
        
        parsed = dict(zip(fields, parts))
        parsed['is_valid'] = True
        parsed['description'] = _describe_cron_schedule(parsed)
        
        return parsed
    
    except Exception as e:
        return {
            'is_valid': False,
            'error': str(e),
            'description': 'Invalid cron expression'
        }

def _describe_cron_schedule(parsed_cron: Dict[str, str]) -> str:
    """Generate human-readable description of cron schedule."""
    # This is a simplified description generator
    # In a production app, you might want to use a library like croniter
    
    minutes = parsed_cron.get('minutes', '*')
    hours = parsed_cron.get('hours', '*')
    day = parsed_cron.get('day', '*')
    month = parsed_cron.get('month', '*')
    weekday = parsed_cron.get('weekday', '*')
    
    if minutes == '0' and hours != '*' and day == '*' and weekday == '*':
        if hours == '*':
            return "Every hour"
        else:
            return f"Daily at {hours}:00"
    elif day == '*' and weekday != '*' and weekday != '?':
        return f"Weekly on day {weekday}"
    elif day != '*' and day != '?' and month == '*':
        return f"Monthly on day {day}"
    else:
        return "Custom schedule"

def get_app_metadata() -> Dict[str, Any]:
    """Get application metadata."""
    return {
        'name': 'DABOps',
        'version': '1.0.0',
        'description': 'Databricks Asset Bundle Operations',
        'author': 'Databricks',
        'license': 'Apache 2.0',
        'repository': 'https://github.com/your-org/dabops',
        'documentation': 'https://docs.databricks.com/dev-tools/bundles/',
        'support_email': 'support@databricks.com'
    }

def load_sample_data() -> Dict[str, Any]:
    """Load sample data for development and testing."""
    return {
        'sample_workflows': [
            {
                'job_id': 12345,
                'name': 'Data Processing Pipeline',
                'description': 'Daily ETL pipeline for customer data',
                'created_time': 1640995200000,  # 2022-01-01
                'creator_user_name': 'data.engineer@company.com',
                'status': 'Active'
            },
            {
                'job_id': 12346,
                'name': 'ML Model Training',
                'description': 'Weekly model retraining job',
                'created_time': 1641081600000,  # 2022-01-02
                'creator_user_name': 'ml.engineer@company.com',
                'status': 'Active'
            }
        ],
        'sample_bundle_template': {
            'bundle': {
                'name': 'sample_bundle',
                'description': 'Sample asset bundle for demonstration'
            },
            'resources': {
                'jobs': {
                    'sample_job': {
                        'name': 'Sample Job',
                        'tasks': [
                            {
                                'task_key': 'sample_task',
                                'notebook_task': {
                                    'notebook_path': '/Users/user@company.com/sample_notebook'
                                }
                            }
                        ]
                    }
                }
            }
        }
    }

class ProgressTracker:
    """Simple progress tracking for long-running operations."""
    
    def __init__(self, total_steps: int, description: str = "Processing"):
        """Initialize progress tracker."""
        self.total_steps = total_steps
        self.current_step = 0
        self.description = description
        self.progress_bar = st.progress(0)
        self.status_text = st.empty()
        self.start_time = datetime.now()
    
    def update(self, step_description: str = ""):
        """Update progress."""
        self.current_step += 1
        progress = self.current_step / self.total_steps
        self.progress_bar.progress(progress)
        
        elapsed = datetime.now() - self.start_time
        if self.current_step > 0:
            eta = elapsed * (self.total_steps - self.current_step) / self.current_step
            eta_str = f"ETA: {str(eta).split('.')[0]}"
        else:
            eta_str = "Calculating..."
        
        status = f"{self.description}: {self.current_step}/{self.total_steps} - {step_description} - {eta_str}"
        self.status_text.text(status)
    
    def complete(self, final_message: str = "Complete"):
        """Mark progress as complete."""
        self.progress_bar.progress(1.0)
        elapsed = datetime.now() - self.start_time
        self.status_text.success(f"{final_message} (completed in {str(elapsed).split('.')[0]})")

def cache_result(ttl_seconds: int = 300):
    """
    Decorator to cache function results with TTL.
    
    Args:
        ttl_seconds: Time to live for cached results
        
    Returns:
        Decorator function
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Use Streamlit's caching mechanism
            @st.cache_data(ttl=ttl_seconds)
            def cached_func(*args, **kwargs):
                return func(*args, **kwargs)
            
            return cached_func(*args, **kwargs)
        
        return wrapper
    
    return decorator

def export_data_as_csv(data: List[Dict[str, Any]], filename: str) -> str:
    """
    Export data as CSV for download.
    
    Args:
        data: List of dictionaries to export
        filename: Name for the CSV file
        
    Returns:
        CSV content as string
    """
    import csv
    import io
    
    if not data:
        return ""
    
    output = io.StringIO()
    fieldnames = data[0].keys()
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    
    writer.writeheader()
    for row in data:
        writer.writerow(row)
    
    return output.getvalue()

# Custom Streamlit components
def render_info_card(title: str, value: str, description: str = "", icon: str = "ℹ️"):
    """Render an information card."""
    with st.container():
        st.markdown(f"""
        <div style="
            background-color: #f0f2f6;
            padding: 1rem;
            border-radius: 0.5rem;
            border-left: 4px solid #1f77b4;
            margin: 0.5rem 0;
        ">
            <h4 style="margin: 0; color: #1f77b4;">
                {icon} {title}
            </h4>
            <h2 style="margin: 0.2rem 0; color: #262730;">
                {value}
            </h2>
            {f'<p style="margin: 0; color: #666; font-size: 0.9rem;">{description}</p>' if description else ''}
        </div>
        """, unsafe_allow_html=True)

def render_status_badge(status: str) -> str:
    """Render a status badge with appropriate color."""
    color_map = {
        'active': '#28a745',
        'inactive': '#6c757d',
        'success': '#28a745',
        'failed': '#dc3545',
        'running': '#007bff',
        'pending': '#ffc107',
        'unknown': '#6c757d'
    }
    
    color = color_map.get(status.lower(), '#6c757d')
    
    return f"""
    <span style="
        background-color: {color};
        color: white;
        padding: 0.2rem 0.5rem;
        border-radius: 0.25rem;
        font-size: 0.8rem;
        font-weight: bold;
    ">
        {status.upper()}
    </span>
    """
