import os
from pathlib import Path
import yaml
from datetime import datetime, timedelta

class Config:
    def __init__(self, config_path=None):
        if config_path is None:
            # Get the project root directory (one level up from src)
            project_root = Path(__file__).parent.parent
            config_path = project_root / "config.yaml"
            
        with open(config_path, 'r') as f:
            self._config = yaml.safe_load(f)
        
        # Process any environment variables in the config
        self._process_env_vars()
        
    def _process_env_vars(self):
        """Replace ${ENV_VAR} placeholders with actual environment variables"""
        def replace_env_vars(obj):
            if isinstance(obj, dict):
                return {k: replace_env_vars(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [replace_env_vars(i) for i in obj]
            elif isinstance(obj, str) and obj.startswith("${") and obj.endswith("}"):
                env_var = obj[2:-1]
                return os.getenv(env_var, "")
            return obj
        
        self._config = replace_env_vars(self._config)
        
    def _get_nested(self, *keys):
        """Safely get nested dictionary values"""
        current = self._config
        for key in keys:
            if current is None:
                return None
            current = current.get(key)
        return current

    @property
    def test_mode(self) -> bool:
        """Whether the bot is running in test mode (no posting to Bluesky)"""
        return self._config.get('test_mode', False)
        
    @property
    def data_dir(self) -> Path:
        return Path(self._get_nested('data', 'data_dir'))

    @property
    def initial_csv_path(self) -> str:
        return self._get_nested('data', 'initial_csv_path')
        
    @property
    def soda_api_url(self) -> str:
        return self._get_nested('data', 'soda_api', 'url')
        
    @property
    def soda_days_to_fetch(self) -> int:
        return self._get_nested('data', 'soda_api', 'days_to_fetch')
        
    @property
    def soda_records_limit(self) -> int:
        return self._get_nested('data', 'soda_api', 'records_limit')
        
    @property
    def soda_params(self) -> dict:
        params = self._get_nested('data', 'soda_api', 'params')
        # Process any date-based parameters
        if params and 'where_clause' in params:
            thirty_days_ago = (datetime.now() - timedelta(days=self.soda_days_to_fetch)).strftime('%Y-%m-%d')
            params['where_clause'] = params['where_clause'].replace('${thirty_days_ago}', thirty_days_ago)
        return params
        
    @property
    def db_file(self) -> str:
        return self._get_nested('database', 'filename')
        
    @property
    def db_backup_enabled(self) -> bool:
        return self._get_nested('database', 'backup', 'enabled')
        
    @property
    def db_backup_retention(self) -> int:
        return self._get_nested('database', 'backup', 'retention_days')
        
    @property
    def db_backup_dir(self) -> Path:
        return Path(self._get_nested('database', 'backup', 'directory'))
        
    @property
    def chart_file(self) -> str:
        return self._get_nested('visualization', 'chart', 'filename')
        
    @property
    def chart_style(self) -> dict:
        return self._get_nested('visualization', 'chart', 'style')
        
    @property
    def chart_colors(self) -> dict:
        return self._get_nested('visualization', 'chart', 'colors')

    @property
    def heatmap_output_dir(self) -> Path:
        return Path(self._get_nested('visualization', 'heatmap', 'output_dir'))

    @property
    def heatmap_emergency_file(self) -> str:
        return self._get_nested('visualization', 'heatmap', 'emergency_filename')

    @property
    def heatmap_style(self) -> dict:
        return self._get_nested('visualization', 'heatmap', 'style')

    @property
    def heatmap_colors(self) -> dict:
        return self._get_nested('visualization', 'heatmap', 'colors')

    @property
    def thread_templates(self) -> dict:
        return self._get_nested('social', 'bluesky', 'thread_templates')

    @property
    def bluesky_post_template(self) -> str:
        """Template for legacy daily update post."""
        return """📊 Chicago Dig Report

Total Permits: {total_tickets}
Emergency Permits: {emergency_tickets} ({emergency_percent}%)
Regular Permits: {regular_tickets}

#ChicagoDigs #Infrastructure"""

    @property
    def day_comparison_settings(self) -> dict:
        return self._get_nested('analytics', 'stats', 'day_comparison')
        
    @property
    def logging_config(self) -> dict:
        return self._get_nested('logging')
        
    @property
    def error_config(self) -> dict:
        return self._get_nested('errors')

# Create global config instance
config = Config()
