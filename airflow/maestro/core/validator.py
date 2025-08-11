"""MAESTRO Configuration Validator"""

from typing import Dict, Any, List
from maestro.utils.logger import get_maestro_logger
from maestro.core.exceptions import ValidationError

logger = get_maestro_logger(__name__)

class ConfigValidator:
    """Validates MAESTRO configuration files"""
   
    REQUIRED_FIELDS = {'country': str, 'verticals': dict}
    VERTICAL_REQUIRED_FIELDS = {'disable': bool, 'mandatory': bool, 'phases': dict}
    PHASE_REQUIRED_FIELDS = {'disable': bool, 'systeme_source': dict}
    SYSTEM_REQUIRED_FIELDS = {'disable': bool, 'flux': list}
    FLUX_REQUIRED_FIELDS = {'name': str, 'mandatory': bool, 'disable': bool}
   
    def validate(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Validate complete configuration"""
        try:
            logger.info("🔍 Starting configuration validation")
            self._validate_root_structure(config)
            self._validate_verticals(config['verticals'])
            self._validate_consistency(config)
            logger.info("✅ Configuration validation passed")
            return config
        except Exception as e:
            error_msg = f"Configuration validation failed: {e}"
            logger.error(f"❌ {error_msg}")
            raise ValidationError(error_msg) from e
   
    def _validate_root_structure(self, config: Dict[str, Any]) -> None:
        for field, field_type in self.REQUIRED_FIELDS.items():
            if field not in config:
                raise ValidationError(f"Missing required field: {field}")
            if not isinstance(config[field], field_type):
                raise ValidationError(f"Field '{field}' must be of type {field_type.__name__}")
        if not config['verticals']:
            raise ValidationError("Verticals section cannot be empty")
   
    def _validate_verticals(self, verticals: Dict[str, Any]) -> None:
        for vertical_name, vertical_config in verticals.items():
            self._validate_vertical(vertical_name, vertical_config)
   
    def _validate_vertical(self, name: str, config: Dict[str, Any]) -> None:
        for field, field_type in self.VERTICAL_REQUIRED_FIELDS.items():
            if field not in config:
                raise ValidationError(f"Vertical '{name}' missing required field: {field}")
            if not isinstance(config[field], field_type):
                raise ValidationError(f"Vertical '{name}' field '{field}' must be of type {field_type.__name__}")
       
        if not config.get('disable', False):
            self._validate_phases(name, config['phases'])
   
    def _validate_phases(self, vertical_name: str, phases: Dict[str, Any]) -> None:
        if not phases:
            logger.warning(f"⚠️ Vertical '{vertical_name}' has no phases defined")
            return
        for phase_name, phase_config in phases.items():
            self._validate_phase(vertical_name, phase_name, phase_config)
   
    def _validate_phase(self, vertical_name: str, phase_name: str, config: Dict[str, Any]) -> None:
        for field, field_type in self.PHASE_REQUIRED_FIELDS.items():
            if field not in config:
                raise ValidationError(f"Phase '{vertical_name}.{phase_name}' missing required field: {field}")
        if not config.get('disable', False):
            self._validate_systems(vertical_name, phase_name, config['systeme_source'])
   
    def _validate_systems(self, vertical_name: str, phase_name: str, systems: Dict[str, Any]) -> None:
        for system_name, system_config in systems.items():
            self._validate_system(vertical_name, phase_name, system_name, system_config)
   
    def _validate_system(self, vertical_name: str, phase_name: str, system_name: str, config: Dict[str, Any]) -> None:
        for field, field_type in self.SYSTEM_REQUIRED_FIELDS.items():
            if field not in config:
                raise ValidationError(f"System '{vertical_name}.{phase_name}.{system_name}' missing required field: {field}")
        if not config.get('disable', False):
            self._validate_flux_list(vertical_name, phase_name, system_name, config['flux'])
   
    def _validate_flux_list(self, vertical_name: str, phase_name: str, system_name: str, flux_list: List[Dict[str, Any]]) -> None:
        if not flux_list:
            logger.warning(f"⚠️ System '{vertical_name}.{phase_name}.{system_name}' has no flux defined")
            return
       
        flux_names = set()
        for i, flux in enumerate(flux_list):
            self._validate_flux(vertical_name, phase_name, system_name, i, flux)
            flux_name = flux.get('name', '')
            if flux_name in flux_names:
                raise ValidationError(f"Duplicate flux name '{flux_name}' in system '{vertical_name}.{phase_name}.{system_name}'")
            flux_names.add(flux_name)
   
    def _validate_flux(self, vertical_name: str, phase_name: str, system_name: str, index: int, flux: Dict[str, Any]) -> None:
        for field, field_type in self.FLUX_REQUIRED_FIELDS.items():
            if field not in flux:
                raise ValidationError(f"Flux #{index} in system '{vertical_name}.{phase_name}.{system_name}' missing required field: {field}")
       
        flux_name = flux['name']
        if not flux_name or not flux_name.strip():
            raise ValidationError(f"Flux #{index} has empty name")
        if not all(c.isalnum() or c in '_-' for c in flux_name):
            raise ValidationError(f"Flux '{flux_name}' contains invalid characters")
   
    def _validate_consistency(self, config: Dict[str, Any]) -> None:
        active_flux = 0
        for vertical_config in config.get('verticals', {}).values():
            if vertical_config.get('disable', False): continue
            for phase_config in vertical_config.get('phases', {}).values():
                if phase_config.get('disable', False): continue
                for system_config in phase_config.get('systeme_source', {}).values():
                    if system_config.get('disable', False): continue
                    for flux in system_config.get('flux', []):
                        if not flux.get('disable', False):
                            active_flux += 1
       
        if active_flux == 0:
            logger.warning("⚠️ All flux are disabled - DAG will be empty")
