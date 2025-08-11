"""MAESTRO Configuration Loader"""

import json
import os
from typing import Dict, Any, Optional
from maestro.utils.logger import get_maestro_logger
from maestro.core.exceptions import ConfigurationError, FileNotFoundError as MaestroFileNotFoundError
from maestro.core.validator import ConfigValidator

logger = get_maestro_logger(__name__)

class ConfigLoader:
    """Centralized configuration loader for MAESTRO framework"""
   
    def __init__(self, base_metadata_path: str = "/opt/airflow/metadata"):
        self.base_metadata_path = base_metadata_path
        self.validator = ConfigValidator()
        self._config_cache = {}
   
    def load_config(self, country: str, vertical: str, config_name: str = "config.json", use_cache: bool = True) -> Dict[str, Any]:
        """Load configuration from metadata structure"""
        cache_key = f"{country}_{vertical}_{config_name}"
       
        if use_cache and cache_key in self._config_cache:
            logger.info(f"📦 Configuration loaded from cache: {cache_key}")
            return self._config_cache[cache_key]
       
        config_path = self._build_config_path(country, vertical, config_name)
        logger.info(f"🔍 Loading configuration from: {config_path}")
       
        try:
            config = self._load_json_file(config_path)
            validated_config = self.validator.validate(config)
           
            if use_cache:
                self._config_cache[cache_key] = validated_config
           
            logger.info(f"✅ Configuration loaded successfully: {country}/{vertical}")
            self._log_config_stats(validated_config)
            return validated_config
           
        except Exception as e:
            logger.error(f"❌ Failed to load configuration: {e}")
            raise
   
    def _build_config_path(self, country: str, vertical: str, config_name: str) -> str:
        return os.path.join(self.base_metadata_path, "scoped", country, vertical, config_name)
   
    def _load_json_file(self, config_path: str) -> Dict[str, Any]:
        if not os.path.exists(config_path):
            raise MaestroFileNotFoundError(f"Configuration file not found: {config_path}")
       
        if not os.access(config_path, os.R_OK):
            raise ConfigurationError(f"Insufficient permissions to read: {config_path}")
       
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            raise ConfigurationError(f"Invalid JSON: {config_path} - Line {e.lineno}, Column {e.colno}: {e.msg}")
        except Exception as e:
            raise ConfigurationError(f"Unexpected error reading {config_path}: {e}")
   
    def _log_config_stats(self, config: Dict[str, Any]) -> None:
        stats = self._calculate_config_stats(config)
        logger.info(f"📊 Configuration Statistics:")
        logger.info(f"   • Country: {stats['country']}")
        logger.info(f"   • Verticals: {stats['verticals_count']}")
        logger.info(f"   • Total flux: {stats['total_flux']}")
        logger.info(f"   • Active flux: {stats['active_flux']}")
        logger.info(f"   • Disabled flux: {stats['disabled_flux']}")
   
    def _calculate_config_stats(self, config: Dict[str, Any]) -> Dict[str, Any]:
        stats = {'country': config.get('country', 'Unknown'), 'verticals_count': len(config.get('verticals', {})), 'total_flux': 0, 'active_flux': 0, 'disabled_flux': 0}
       
        for vertical_config in config.get('verticals', {}).values():
            if vertical_config.get('disable', False): continue
            for phase_config in vertical_config.get('phases', {}).values():
                if phase_config.get('disable', False): continue
                for system_config in phase_config.get('systeme_source', {}).values():
                    if system_config.get('disable', False): continue
                    for flux in system_config.get('flux', []):
                        stats['total_flux'] += 1
                        if flux.get('disable', False):
                            stats['disabled_flux'] += 1
                        else:
                            stats['active_flux'] += 1
        return stats
   
    def clear_cache(self) -> None:
        self._config_cache.clear()
        logger.info("🧹 Configuration cache cleared")
