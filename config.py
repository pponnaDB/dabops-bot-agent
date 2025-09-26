"""
Configuration Management Module
Handles application configuration and settings for DABOps.
"""

import os
import json
import logging
from typing import Dict, Any, Optional
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

@dataclass
class AppConfig:
    """Application configuration class."""
    
    # App metadata
    app_name: str = "DABOps"
    app_version: str = "1.0.0"
    app_description: str = "Databricks Asset Bundle Operations"
    
    # Databricks configuration
    databricks_host: Optional[str] = None
    databricks_token: Optional[str] = None
    databricks_profile: str = "DEFAULT"
    
    # UI configuration
    max_workflows_display: int = 100
    default_bundle_format: str = "yaml"
    auto_save_bundles: bool = True
    theme: str = "light"
    
    # Bundle generation settings
    default_target_env: str = "dev"
    include_dependencies: bool = True
    bundle_output_dir: str = "/Workspace/Users/{user}/DABOps/bundles"
    
    # Logging configuration
    log_level: str = "INFO"
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    log_file: Optional[str] = None
    
    # Feature flags
    enable_advanced_features: bool = False
    enable_debug_mode: bool = False
    enable_telemetry: bool = True
    
    # Cache settings
    cache_ttl_seconds: int = 300  # 5 minutes
    enable_caching: bool = True
    
    # Security settings
    allowed_file_extensions: list = field(default_factory=lambda: ['.yml', '.yaml', '.json'])
    max_file_size_mb: int = 10
    
    def __post_init__(self):
        """Initialize configuration from environment variables."""
        self._load_from_environment()
        self._load_from_file()
        self._validate_configuration()
    
    def _load_from_environment(self):
        """Load configuration from environment variables."""
        env_mappings = {
            'DATABRICKS_HOST': 'databricks_host',
            'DATABRICKS_TOKEN': 'databricks_token',
            'DATABRICKS_CONFIG_PROFILE': 'databricks_profile',
            'DABOPS_LOG_LEVEL': 'log_level',
            'DABOPS_MAX_WORKFLOWS': 'max_workflows_display',
            'DABOPS_BUNDLE_FORMAT': 'default_bundle_format',
            'DABOPS_AUTO_SAVE': 'auto_save_bundles',
            'DABOPS_THEME': 'theme',
            'DABOPS_CACHE_TTL': 'cache_ttl_seconds',
            'DABOPS_DEBUG': 'enable_debug_mode',
            'DABOPS_TELEMETRY': 'enable_telemetry'
        }
        
        for env_var, attr_name in env_mappings.items():
            env_value = os.getenv(env_var)
            if env_value is not None:
                # Convert string values to appropriate types
                if attr_name in ['max_workflows_display', 'cache_ttl_seconds']:
                    try:
                        env_value = int(env_value)
                    except ValueError:
                        logger.warning(f"Invalid integer value for {env_var}: {env_value}")
                        continue
                elif attr_name in ['auto_save_bundles', 'enable_debug_mode', 'enable_telemetry']:
                    env_value = env_value.lower() in ('true', '1', 'yes', 'on')
                
                setattr(self, attr_name, env_value)
                logger.debug(f"Loaded {attr_name} from environment: {env_value}")
    
    def _load_from_file(self):
        """Load configuration from config file if exists."""
        config_paths = [
            os.path.join(os.getcwd(), 'config.json'),
            os.path.expanduser('~/.dabops/config.json'),
            '/etc/dabops/config.json'
        ]
        
        for config_path in config_paths:
            if os.path.exists(config_path):
                try:
                    with open(config_path, 'r') as f:
                        file_config = json.load(f)
                    
                    for key, value in file_config.items():
                        if hasattr(self, key):
                            setattr(self, key, value)
                            logger.debug(f"Loaded {key} from config file: {value}")
                    
                    logger.info(f"Loaded configuration from: {config_path}")
                    break
                    
                except (json.JSONDecodeError, IOError) as e:
                    logger.warning(f"Failed to load config from {config_path}: {str(e)}")
    
    def _validate_configuration(self):
        """Validate configuration values."""
        # Validate log level
        valid_log_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if self.log_level.upper() not in valid_log_levels:
            logger.warning(f"Invalid log level '{self.log_level}', using INFO")
            self.log_level = "INFO"
        
        # Validate bundle format
        valid_formats = ['yaml', 'json']
        if self.default_bundle_format.lower() not in valid_formats:
            logger.warning(f"Invalid bundle format '{self.default_bundle_format}', using yaml")
            self.default_bundle_format = "yaml"
        
        # Validate theme
        valid_themes = ['light', 'dark', 'auto']
        if self.theme.lower() not in valid_themes:
            logger.warning(f"Invalid theme '{self.theme}', using light")
            self.theme = "light"
        
        # Validate numeric values
        if self.max_workflows_display <= 0:
            self.max_workflows_display = 100
        
        if self.cache_ttl_seconds <= 0:
            self.cache_ttl_seconds = 300
        
        if self.max_file_size_mb <= 0:
            self.max_file_size_mb = 10
        
        logger.debug("Configuration validation completed")
    
    def get_databricks_config(self) -> Dict[str, Any]:
        """Get Databricks-specific configuration."""
        return {
            'host': self.databricks_host,
            'token': self.databricks_token,
            'profile': self.databricks_profile
        }
    
    def get_ui_config(self) -> Dict[str, Any]:
        """Get UI-specific configuration."""
        return {
            'max_workflows_display': self.max_workflows_display,
            'theme': self.theme,
            'auto_save_bundles': self.auto_save_bundles,
            'default_bundle_format': self.default_bundle_format
        }
    
    def get_bundle_config(self) -> Dict[str, Any]:
        """Get bundle generation configuration."""
        return {
            'default_target_env': self.default_target_env,
            'include_dependencies': self.include_dependencies,
            'output_dir': self.bundle_output_dir,
            'allowed_extensions': self.allowed_file_extensions,
            'max_file_size_mb': self.max_file_size_mb
        }
    
    def get_logging_config(self) -> Dict[str, Any]:
        """Get logging configuration."""
        return {
            'level': self.log_level,
            'format': self.log_format,
            'file': self.log_file
        }
    
    def save_to_file(self, file_path: str):
        """Save current configuration to file."""
        try:
            config_dict = {
                'app_name': self.app_name,
                'app_version': self.app_version,
                'max_workflows_display': self.max_workflows_display,
                'default_bundle_format': self.default_bundle_format,
                'auto_save_bundles': self.auto_save_bundles,
                'theme': self.theme,
                'default_target_env': self.default_target_env,
                'include_dependencies': self.include_dependencies,
                'log_level': self.log_level,
                'cache_ttl_seconds': self.cache_ttl_seconds,
                'enable_debug_mode': self.enable_debug_mode,
                'enable_telemetry': self.enable_telemetry,
                'max_file_size_mb': self.max_file_size_mb
            }
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            with open(file_path, 'w') as f:
                json.dump(config_dict, f, indent=2)
            
            logger.info(f"Configuration saved to: {file_path}")
            
        except IOError as e:
            logger.error(f"Failed to save configuration to {file_path}: {str(e)}")
    
    def update_from_dict(self, updates: Dict[str, Any]):
        """Update configuration from dictionary."""
        for key, value in updates.items():
            if hasattr(self, key):
                setattr(self, key, value)
                logger.debug(f"Updated {key} to: {value}")
            else:
                logger.warning(f"Unknown configuration key: {key}")
        
        self._validate_configuration()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            'app_name': self.app_name,
            'app_version': self.app_version,
            'app_description': self.app_description,
            'databricks_profile': self.databricks_profile,
            'max_workflows_display': self.max_workflows_display,
            'default_bundle_format': self.default_bundle_format,
            'auto_save_bundles': self.auto_save_bundles,
            'theme': self.theme,
            'default_target_env': self.default_target_env,
            'include_dependencies': self.include_dependencies,
            'bundle_output_dir': self.bundle_output_dir,
            'log_level': self.log_level,
            'cache_ttl_seconds': self.cache_ttl_seconds,
            'enable_advanced_features': self.enable_advanced_features,
            'enable_debug_mode': self.enable_debug_mode,
            'enable_telemetry': self.enable_telemetry,
            'max_file_size_mb': self.max_file_size_mb,
            'allowed_file_extensions': self.allowed_file_extensions
        }
    
    def get_feature_flags(self) -> Dict[str, bool]:
        """Get feature flags."""
        return {
            'advanced_features': self.enable_advanced_features,
            'debug_mode': self.enable_debug_mode,
            'telemetry': self.enable_telemetry,
            'caching': self.enable_caching
        }
    
    def __str__(self) -> str:
        """String representation of configuration."""
        return f"DABOps Config v{self.app_version} (Profile: {self.databricks_profile})"
    
    def __repr__(self) -> str:
        """Detailed representation of configuration."""
        return f"AppConfig(name={self.app_name}, version={self.app_version}, profile={self.databricks_profile})"
