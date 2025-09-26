"""
Databricks Client Module
Handles all Databricks API interactions for the DABOps application.
"""

import os
import json
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import Job
from databricks.sdk.service.workspace import ObjectInfo, ObjectType
from databricks.sdk.core import DatabricksError

from config import AppConfig
from utils import handle_databricks_error

logger = logging.getLogger(__name__)

class DatabricksClient:
    """Client for interacting with Databricks APIs."""
    
    def __init__(self):
        """Initialize the Databricks client."""
        self.config = AppConfig()
        self.client = None
        self.current_user = None
        self._initialize_client()
    
    def _initialize_client(self):
        """Initialize the Databricks workspace client."""
        try:
            # Initialize the SDK client
            # The SDK will automatically use authentication from:
            # 1. Environment variables (DATABRICKS_HOST, DATABRICKS_TOKEN)
            # 2. Databricks CLI configuration (~/.databrickscfg)
            # 3. Azure CLI (for Azure Databricks)
            # 4. Service principal authentication
            
            self.client = WorkspaceClient()
            
            # Get current user information
            self.current_user = self.client.current_user.me().user_name
            logger.info(f"Successfully initialized Databricks client for user: {self.current_user}")
            
        except Exception as e:
            logger.error(f"Failed to initialize Databricks client: {str(e)}")
            self.client = None
            raise DatabricksError(f"Authentication failed: {str(e)}")
    
    def is_authenticated(self) -> bool:
        """Check if the client is properly authenticated."""
        return self.client is not None and self.current_user is not None
    
    def get_workspace_info(self) -> Optional[Dict[str, Any]]:
        """Get workspace information."""
        if not self.is_authenticated():
            return None
        
        try:
            # Get workspace configuration
            workspace_conf = self.client.config
            
            return {
                'workspace_url': workspace_conf.host,
                'current_user': self.current_user,
                'auth_type': workspace_conf.auth_type,
            }
        except Exception as e:
            logger.error(f"Failed to get workspace info: {str(e)}")
            return None
    
    @handle_databricks_error
    def list_workflows(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        List all workflows (jobs) in the workspace.
        
        Args:
            limit: Maximum number of workflows to return
            
        Returns:
            List of workflow information dictionaries
        """
        if not self.is_authenticated():
            raise DatabricksError("Client not authenticated")
        
        workflows = []
        
        try:
            # List all jobs using the SDK
            jobs = list(self.client.jobs.list(limit=limit))
            
            for job in jobs:
                workflow_info = {
                    'job_id': job.job_id,
                    'name': job.settings.name if job.settings else 'Unnamed Job',
                    'description': getattr(job.settings, 'description', '') if job.settings else '',
                    'created_time': job.created_time,
                    'modified_time': getattr(job, 'modified_time', job.created_time),
                    'creator_user_name': job.creator_user_name,
                    'status': 'Active',  # Jobs API doesn't provide status directly
                    'job_type': getattr(job.settings, 'job_type', 'Unknown') if job.settings else 'Unknown',
                    'timeout_seconds': getattr(job.settings, 'timeout_seconds', None) if job.settings else None,
                    'max_concurrent_runs': getattr(job.settings, 'max_concurrent_runs', 1) if job.settings else 1,
                    'tags': getattr(job.settings, 'tags', {}) if job.settings else {},
                    'tasks': self._extract_task_info(job.settings) if job.settings else []
                }
                
                workflows.append(workflow_info)
            
            logger.info(f"Retrieved {len(workflows)} workflows from workspace")
            return workflows
            
        except Exception as e:
            logger.error(f"Failed to list workflows: {str(e)}")
            raise DatabricksError(f"Failed to retrieve workflows: {str(e)}")
    
    def _extract_task_info(self, job_settings) -> List[Dict[str, Any]]:
        """Extract task information from job settings."""
        tasks = []
        
        if hasattr(job_settings, 'tasks') and job_settings.tasks:
            for task in job_settings.tasks:
                task_info = {
                    'task_key': task.task_key,
                    'description': getattr(task, 'description', ''),
                    'depends_on': [dep.task_key for dep in getattr(task, 'depends_on', [])],
                    'timeout_seconds': getattr(task, 'timeout_seconds', None),
                }
                
                # Add task type information
                if hasattr(task, 'notebook_task') and task.notebook_task:
                    task_info['type'] = 'notebook'
                    task_info['notebook_path'] = task.notebook_task.notebook_path
                elif hasattr(task, 'spark_jar_task') and task.spark_jar_task:
                    task_info['type'] = 'jar'
                    task_info['main_class_name'] = task.spark_jar_task.main_class_name
                elif hasattr(task, 'python_wheel_task') and task.python_wheel_task:
                    task_info['type'] = 'python_wheel'
                    task_info['package_name'] = task.python_wheel_task.package_name
                elif hasattr(task, 'spark_submit_task') and task.spark_submit_task:
                    task_info['type'] = 'spark_submit'
                elif hasattr(task, 'pipeline_task') and task.pipeline_task:
                    task_info['type'] = 'pipeline'
                    task_info['pipeline_id'] = task.pipeline_task.pipeline_id
                else:
                    task_info['type'] = 'unknown'
                
                tasks.append(task_info)
        
        return tasks
    
    @handle_databricks_error
    def get_workflow_details(self, job_id: int) -> Optional[Dict[str, Any]]:
        """
        Get detailed information about a specific workflow.
        
        Args:
            job_id: The job ID to get details for
            
        Returns:
            Detailed workflow information dictionary
        """
        if not self.is_authenticated():
            raise DatabricksError("Client not authenticated")
        
        try:
            job = self.client.jobs.get(job_id)
            
            # Get run history
            runs = list(self.client.jobs.list_runs(job_id=job_id, limit=10))
            
            detailed_info = {
                'job_id': job.job_id,
                'name': job.settings.name if job.settings else 'Unnamed Job',
                'description': getattr(job.settings, 'description', '') if job.settings else '',
                'created_time': job.created_time,
                'creator_user_name': job.creator_user_name,
                'settings': job.settings,
                'recent_runs': [
                    {
                        'run_id': run.run_id,
                        'start_time': run.start_time,
                        'end_time': run.end_time,
                        'state': run.state.life_cycle_state if run.state else 'Unknown',
                        'result_state': run.state.result_state if run.state else 'Unknown'
                    }
                    for run in runs[:5]  # Last 5 runs
                ]
            }
            
            return detailed_info
            
        except Exception as e:
            logger.error(f"Failed to get workflow details for job_id {job_id}: {str(e)}")
            return None
    
    @handle_databricks_error
    def save_file_to_workspace(self, content: str, path: str) -> bool:
        """
        Save content to a workspace file.
        
        Args:
            content: The content to save
            path: The workspace path to save to
            
        Returns:
            True if successful, False otherwise
        """
        if not self.is_authenticated():
            raise DatabricksError("Client not authenticated")
        
        try:
            # Ensure the directory exists
            directory = '/'.join(path.split('/')[:-1])
            self._ensure_workspace_directory(directory)
            
            # Save the file
            self.client.workspace.upload(
                path=path,
                content=content.encode('utf-8'),
                format=ObjectType.FILE,
                overwrite=True
            )
            
            logger.info(f"Successfully saved file to workspace: {path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save file to workspace {path}: {str(e)}")
            return False
    
    def _ensure_workspace_directory(self, directory: str):
        """Ensure a workspace directory exists."""
        try:
            # Try to get the directory info
            self.client.workspace.get_status(directory)
        except:
            # Directory doesn't exist, create it
            try:
                self.client.workspace.mkdirs(directory)
                logger.info(f"Created workspace directory: {directory}")
            except Exception as e:
                logger.warning(f"Failed to create directory {directory}: {str(e)}")
    
    @handle_databricks_error
    def get_workspace_files(self, path: str) -> List[Dict[str, Any]]:
        """
        List files in a workspace directory.
        
        Args:
            path: The workspace path to list
            
        Returns:
            List of file information dictionaries
        """
        if not self.is_authenticated():
            raise DatabricksError("Client not authenticated")
        
        try:
            objects = list(self.client.workspace.list(path))
            
            files = []
            for obj in objects:
                files.append({
                    'path': obj.path,
                    'object_type': obj.object_type.name,
                    'size': getattr(obj, 'size', 0),
                    'modified_at': getattr(obj, 'modified_at', None),
                    'language': getattr(obj, 'language', None)
                })
            
            return files
            
        except Exception as e:
            logger.error(f"Failed to list workspace files in {path}: {str(e)}")
            return []
    
    def test_connection(self) -> Dict[str, Any]:
        """
        Test the Databricks connection and return status information.
        
        Returns:
            Dictionary with connection test results
        """
        try:
            if not self.is_authenticated():
                return {
                    'status': 'failed',
                    'message': 'Not authenticated',
                    'details': None
                }
            
            # Try to get current user info
            user = self.client.current_user.me()
            workspace_info = self.get_workspace_info()
            
            return {
                'status': 'success',
                'message': 'Connection successful',
                'details': {
                    'user': user.user_name,
                    'workspace_url': workspace_info.get('workspace_url') if workspace_info else 'Unknown',
                    'auth_type': workspace_info.get('auth_type') if workspace_info else 'Unknown'
                }
            }
            
        except Exception as e:
            return {
                'status': 'failed',
                'message': f'Connection test failed: {str(e)}',
                'details': str(e)
            }
