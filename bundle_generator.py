"""
Bundle Generator Module
Handles asset bundle generation for Databricks workflows.
"""

import os
import json
import yaml
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime

from databricks_client import DatabricksClient
from config import AppConfig

logger = logging.getLogger(__name__)

class BundleGenerator:
    """Generates Databricks Asset Bundles from workflow definitions."""
    
    def __init__(self, databricks_client: DatabricksClient):
        """Initialize the bundle generator."""
        self.db_client = databricks_client
        self.config = AppConfig()
    
    def generate_bundle(
        self, 
        workflow: Dict[str, Any], 
        bundle_name: str,
        include_dependencies: bool = True,
        target_environment: str = "dev"
    ) -> Optional[str]:
        """
        Generate an asset bundle YAML for the given workflow.
        
        Args:
            workflow: Workflow information dictionary
            bundle_name: Name for the bundle
            include_dependencies: Whether to include dependencies
            target_environment: Target environment for the bundle
            
        Returns:
            Generated bundle YAML as string, or None if failed
        """
        try:
            logger.info(f"Generating asset bundle for workflow: {workflow.get('name', 'Unknown')}")
            
            # Get detailed workflow information
            detailed_workflow = self.db_client.get_workflow_details(workflow['job_id'])
            if not detailed_workflow:
                logger.error("Failed to get detailed workflow information")
                return None
            
            # Create bundle structure
            bundle = self._create_bundle_structure(
                detailed_workflow, 
                bundle_name,
                target_environment
            )
            
            # Add workflow resources
            self._add_workflow_resources(bundle, detailed_workflow)
            
            # Add dependencies if requested
            if include_dependencies:
                self._add_dependencies(bundle, detailed_workflow)
            
            # Convert to YAML
            bundle_yaml = self._convert_to_yaml(bundle)
            
            logger.info(f"Successfully generated asset bundle: {bundle_name}")
            return bundle_yaml
            
        except Exception as e:
            logger.error(f"Failed to generate bundle: {str(e)}")
            return None

    def generate_resources_only(
        self, 
        workflow: Dict[str, Any], 
        include_dependencies: bool = True
    ) -> Optional[str]:
        """
        Generate only the resources section for the given workflow.
        
        Args:
            workflow: Workflow information dictionary
            include_dependencies: Whether to include dependencies
            
        Returns:
            Generated resources YAML as string, or None if failed
        """
        try:
            logger.info(f"Generating resources-only for workflow: {workflow.get('name', 'Unknown')}")
            
            # Get detailed workflow information
            detailed_workflow = self.db_client.get_workflow_details(workflow['job_id'])
            if not detailed_workflow:
                logger.error("Failed to get detailed workflow information")
                return None
            
            # Create only resources structure
            resources = {'resources': {'jobs': {}}}
            
            # Add workflow resources
            self._add_workflow_resources(resources, detailed_workflow)
            
            # Add dependencies if requested
            if include_dependencies:
                self._add_dependencies(resources, detailed_workflow)
            
            # Convert to YAML
            resources_yaml = self._convert_resources_to_yaml(resources, workflow)
            
            logger.info(f"Successfully generated resources for workflow: {workflow.get('name', 'Unknown')}")
            return resources_yaml
            
        except Exception as e:
            logger.error(f"Failed to generate resources: {str(e)}")
            return None
    
    def _create_bundle_structure(
        self, 
        workflow: Dict[str, Any], 
        bundle_name: str,
        target_environment: str
    ) -> Dict[str, Any]:
        """Create the basic bundle structure."""
        current_user = self.db_client.current_user or "unknown_user"
        
        bundle = {
            'bundle': {
                'name': bundle_name,
                'description': f"Asset bundle for workflow: {workflow.get('name', 'Unknown')}",
                'git': {
                    'origin_url': '${var.git_origin_url}',
                    'branch': '${var.git_branch}'
                }
            },
            'variables': {
                'git_origin_url': {
                    'description': 'Git repository origin URL',
                    'default': 'https://github.com/your-org/your-repo.git'
                },
                'git_branch': {
                    'description': 'Git branch to use',
                    'default': 'main'
                }
            },
            'targets': {
                target_environment: {
                    'mode': 'development' if target_environment == 'dev' else 'production',
                    'default': target_environment == 'dev',
                    'workspace': {
                        'host': '${var.workspace_host}',
                        'current_user': {
                            'user_name': current_user
                        }
                    },
                    'variables': {
                        'workspace_host': {
                            'description': 'Databricks workspace host URL',
                            'default': self.db_client.get_workspace_info().get('workspace_url', '')
                        }
                    }
                }
            },
            'resources': {
                'jobs': {}
            }
        }
        
        # Add additional target environments
        if target_environment == 'dev':
            bundle['targets']['staging'] = {
                'mode': 'development',
                'workspace': {
                    'host': '${var.staging_workspace_host}'
                },
                'variables': {
                    'staging_workspace_host': {
                        'description': 'Staging workspace host URL'
                    }
                }
            }
            
            bundle['targets']['prod'] = {
                'mode': 'production',
                'workspace': {
                    'host': '${var.prod_workspace_host}'
                },
                'variables': {
                    'prod_workspace_host': {
                        'description': 'Production workspace host URL'
                    }
                }
            }
        
        return bundle
    
    def _add_workflow_resources(self, bundle: Dict[str, Any], workflow: Dict[str, Any]):
        """Add workflow resources to the bundle."""
        job_settings = workflow.get('settings')
        if not job_settings:
            logger.warning("No job settings found for workflow")
            return
        
        job_name = workflow.get('name', 'unnamed_job').lower().replace(' ', '_').replace('-', '_')
        
        # Convert job settings to bundle format
        job_resource = {
            'name': workflow.get('name', 'Unnamed Job'),
            'description': workflow.get('description', ''),
            'tags': getattr(job_settings, 'tags', {}),
            'timeout_seconds': getattr(job_settings, 'timeout_seconds', None),
            'max_concurrent_runs': getattr(job_settings, 'max_concurrent_runs', 1),
            'email_notifications': self._convert_email_notifications(job_settings),
            'webhook_notifications': self._convert_webhook_notifications(job_settings),
            'schedule': self._convert_schedule(job_settings),
            'job_clusters': self._convert_job_clusters(job_settings),
            'tasks': self._convert_tasks(job_settings)
        }
        
        # Remove None values
        job_resource = {k: v for k, v in job_resource.items() if v is not None}
        
        bundle['resources']['jobs'][job_name] = job_resource
    
    def _convert_email_notifications(self, job_settings) -> Optional[Dict[str, Any]]:
        """Convert email notifications to bundle format."""
        if not hasattr(job_settings, 'email_notifications') or not job_settings.email_notifications:
            return None
        
        notifications = job_settings.email_notifications
        return {
            'on_start': getattr(notifications, 'on_start', []),
            'on_success': getattr(notifications, 'on_success', []),
            'on_failure': getattr(notifications, 'on_failure', []),
            'no_alert_for_skipped_runs': getattr(notifications, 'no_alert_for_skipped_runs', False)
        }
    
    def _convert_webhook_notifications(self, job_settings) -> Optional[Dict[str, Any]]:
        """Convert webhook notifications to bundle format."""
        if not hasattr(job_settings, 'webhook_notifications') or not job_settings.webhook_notifications:
            return None
        
        notifications = job_settings.webhook_notifications
        return {
            'on_start': [{'id': wh.id} for wh in getattr(notifications, 'on_start', [])],
            'on_success': [{'id': wh.id} for wh in getattr(notifications, 'on_success', [])],
            'on_failure': [{'id': wh.id} for wh in getattr(notifications, 'on_failure', [])],
        }
    
    def _convert_schedule(self, job_settings) -> Optional[Dict[str, Any]]:
        """Convert schedule to bundle format."""
        if not hasattr(job_settings, 'schedule') or not job_settings.schedule:
            return None
        
        schedule = job_settings.schedule
        return {
            'quartz_cron_expression': schedule.quartz_cron_expression,
            'timezone_id': schedule.timezone_id,
            'pause_status': getattr(schedule, 'pause_status', 'UNPAUSED')
        }
    
    def _convert_job_clusters(self, job_settings) -> Optional[List[Dict[str, Any]]]:
        """Convert job clusters to bundle format."""
        if not hasattr(job_settings, 'job_clusters') or not job_settings.job_clusters:
            return None
        
        clusters = []
        for cluster in job_settings.job_clusters:
            cluster_config = {
                'job_cluster_key': cluster.job_cluster_key,
                'new_cluster': {
                    'spark_version': cluster.new_cluster.spark_version,
                    'node_type_id': cluster.new_cluster.node_type_id,
                    'num_workers': getattr(cluster.new_cluster, 'num_workers', 1),
                    'autoscale': getattr(cluster.new_cluster, 'autoscale', None),
                    'spark_conf': getattr(cluster.new_cluster, 'spark_conf', {}),
                    'spark_env_vars': getattr(cluster.new_cluster, 'spark_env_vars', {}),
                    'custom_tags': getattr(cluster.new_cluster, 'custom_tags', {}),
                    'init_scripts': getattr(cluster.new_cluster, 'init_scripts', []),
                    'driver_node_type_id': getattr(cluster.new_cluster, 'driver_node_type_id', None),
                    'ssh_public_keys': getattr(cluster.new_cluster, 'ssh_public_keys', []),
                    'cluster_log_conf': getattr(cluster.new_cluster, 'cluster_log_conf', None),
                    'enable_elastic_disk': getattr(cluster.new_cluster, 'enable_elastic_disk', None),
                    'disk_spec': getattr(cluster.new_cluster, 'disk_spec', None),
                    'cluster_mount_infos': getattr(cluster.new_cluster, 'cluster_mount_infos', [])
                }
            }
            
            # Clean up None values
            cluster_config['new_cluster'] = {
                k: v for k, v in cluster_config['new_cluster'].items() 
                if v is not None and v != [] and v != {}
            }
            
            clusters.append(cluster_config)
        
        return clusters
    
    def _convert_tasks(self, job_settings) -> Optional[List[Dict[str, Any]]]:
        """Convert tasks to bundle format."""
        if not hasattr(job_settings, 'tasks') or not job_settings.tasks:
            return None
        
        tasks = []
        for task in job_settings.tasks:
            task_config = {
                'task_key': task.task_key,
                'description': getattr(task, 'description', ''),
                'depends_on': [{'task_key': dep.task_key} for dep in getattr(task, 'depends_on', [])],
                'timeout_seconds': getattr(task, 'timeout_seconds', None),
                'max_retries': getattr(task, 'max_retries', None),
                'min_retry_interval_millis': getattr(task, 'min_retry_interval_millis', None),
                'retry_on_timeout': getattr(task, 'retry_on_timeout', None)
            }
            
            # Add task type specific configuration
            if hasattr(task, 'notebook_task') and task.notebook_task:
                task_config['notebook_task'] = {
                    'notebook_path': task.notebook_task.notebook_path,
                    'source': getattr(task.notebook_task, 'source', 'WORKSPACE'),
                    'base_parameters': getattr(task.notebook_task, 'base_parameters', {})
                }
            
            elif hasattr(task, 'python_wheel_task') and task.python_wheel_task:
                task_config['python_wheel_task'] = {
                    'package_name': task.python_wheel_task.package_name,
                    'entry_point': task.python_wheel_task.entry_point,
                    'parameters': getattr(task.python_wheel_task, 'parameters', []),
                    'named_parameters': getattr(task.python_wheel_task, 'named_parameters', {})
                }
            
            elif hasattr(task, 'spark_jar_task') and task.spark_jar_task:
                task_config['spark_jar_task'] = {
                    'main_class_name': task.spark_jar_task.main_class_name,
                    'parameters': getattr(task.spark_jar_task, 'parameters', [])
                }
            
            elif hasattr(task, 'spark_python_task') and task.spark_python_task:
                task_config['spark_python_task'] = {
                    'python_file': task.spark_python_task.python_file,
                    'parameters': getattr(task.spark_python_task, 'parameters', []),
                    'source': getattr(task.spark_python_task, 'source', 'WORKSPACE')
                }
            
            elif hasattr(task, 'spark_submit_task') and task.spark_submit_task:
                task_config['spark_submit_task'] = {
                    'parameters': task.spark_submit_task.parameters
                }
            
            elif hasattr(task, 'pipeline_task') and task.pipeline_task:
                task_config['pipeline_task'] = {
                    'pipeline_id': task.pipeline_task.pipeline_id,
                    'full_refresh': getattr(task.pipeline_task, 'full_refresh', False)
                }
            
            elif hasattr(task, 'sql_task') and task.sql_task:
                task_config['sql_task'] = {
                    'query': getattr(task.sql_task, 'query', None),
                    'dashboard': getattr(task.sql_task, 'dashboard', None),
                    'alert': getattr(task.sql_task, 'alert', None),
                    'warehouse_id': task.sql_task.warehouse_id,
                    'parameters': getattr(task.sql_task, 'parameters', {})
                }
            
            # Add compute configuration
            if hasattr(task, 'job_cluster_key') and task.job_cluster_key:
                task_config['job_cluster_key'] = task.job_cluster_key
            elif hasattr(task, 'existing_cluster_id') and task.existing_cluster_id:
                task_config['existing_cluster_id'] = task.existing_cluster_id
            elif hasattr(task, 'new_cluster') and task.new_cluster:
                task_config['new_cluster'] = self._convert_cluster_config(task.new_cluster)
            
            # Add libraries
            if hasattr(task, 'libraries') and task.libraries:
                task_config['libraries'] = self._convert_libraries(task.libraries)
            
            # Clean up None values
            task_config = {k: v for k, v in task_config.items() if v is not None and v != [] and v != {}}
            
            tasks.append(task_config)
        
        return tasks
    
    def _convert_cluster_config(self, cluster) -> Dict[str, Any]:
        """Convert cluster configuration to bundle format."""
        config = {
            'spark_version': cluster.spark_version,
            'node_type_id': cluster.node_type_id,
            'num_workers': getattr(cluster, 'num_workers', 1),
            'autoscale': getattr(cluster, 'autoscale', None),
            'spark_conf': getattr(cluster, 'spark_conf', {}),
            'spark_env_vars': getattr(cluster, 'spark_env_vars', {}),
            'custom_tags': getattr(cluster, 'custom_tags', {}),
        }
        
        return {k: v for k, v in config.items() if v is not None and v != {} and v != []}
    
    def _convert_libraries(self, libraries) -> List[Dict[str, Any]]:
        """Convert libraries to bundle format."""
        converted_libs = []
        
        for lib in libraries:
            lib_config = {}
            
            if hasattr(lib, 'jar') and lib.jar:
                lib_config['jar'] = lib.jar
            elif hasattr(lib, 'egg') and lib.egg:
                lib_config['egg'] = lib.egg
            elif hasattr(lib, 'whl') and lib.whl:
                lib_config['whl'] = lib.whl
            elif hasattr(lib, 'pypi') and lib.pypi:
                lib_config['pypi'] = {
                    'package': lib.pypi.package,
                    'repo': getattr(lib.pypi, 'repo', None)
                }
                lib_config['pypi'] = {k: v for k, v in lib_config['pypi'].items() if v is not None}
            elif hasattr(lib, 'maven') and lib.maven:
                lib_config['maven'] = {
                    'coordinates': lib.maven.coordinates,
                    'repo': getattr(lib.maven, 'repo', None),
                    'exclusions': getattr(lib.maven, 'exclusions', [])
                }
                lib_config['maven'] = {k: v for k, v in lib_config['maven'].items() if v is not None and v != []}
            elif hasattr(lib, 'cran') and lib.cran:
                lib_config['cran'] = {
                    'package': lib.cran.package,
                    'repo': getattr(lib.cran, 'repo', None)
                }
                lib_config['cran'] = {k: v for k, v in lib_config['cran'].items() if v is not None}
            
            if lib_config:
                converted_libs.append(lib_config)
        
        return converted_libs
    
    def _add_dependencies(self, bundle: Dict[str, Any], workflow: Dict[str, Any]):
        """Add workflow dependencies to the bundle."""
        # This is a placeholder for dependency analysis
        # In a real implementation, you would analyze the workflow tasks
        # and identify dependencies like notebooks, libraries, etc.
        
        logger.info("Dependency analysis not implemented yet")
        pass
    
    def _convert_to_yaml(self, bundle: Dict[str, Any]) -> str:
        """Convert bundle dictionary to YAML string."""
        try:
            # Add header comment
            header = f"""# Databricks Asset Bundle Configuration
# Generated on: {datetime.now().isoformat()}
# Bundle: {bundle.get('bundle', {}).get('name', 'Unknown')}
# Description: {bundle.get('bundle', {}).get('description', 'Auto-generated asset bundle')}

"""
            
            # Convert to YAML with proper formatting
            yaml_content = yaml.dump(
                bundle,
                default_flow_style=False,
                indent=2,
                sort_keys=False,
                allow_unicode=True
            )
            
            return header + yaml_content
            
        except Exception as e:
            logger.error(f"Failed to convert bundle to YAML: {str(e)}")
            return f"# Error generating YAML: {str(e)}"

    def _convert_resources_to_yaml(self, resources: Dict[str, Any], workflow: Dict[str, Any]) -> str:
        """Convert resources dictionary to YAML string."""
        try:
            workflow_name = workflow.get('name', 'Unknown')
            job_id = workflow.get('job_id', 'N/A')
            
            # Add header comment for resources-only file
            header = f"""# Databricks Asset Bundle Resources
# Generated on: {datetime.now().isoformat()}
# Workflow: {workflow_name}
# Job ID: {job_id}
# Contains only the 'resources:' section for this workflow

"""
            
            # Convert to YAML with proper formatting
            yaml_content = yaml.dump(
                resources,
                default_flow_style=False,
                indent=2,
                sort_keys=False,
                allow_unicode=True
            )
            
            return header + yaml_content
            
        except Exception as e:
            logger.error(f"Failed to convert resources to YAML: {str(e)}")
            return f"# Error generating YAML: {str(e)}"
