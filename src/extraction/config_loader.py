"""
Configuration loader for extraction rules and settings.
"""

import yaml
from pathlib import Path
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

def load_extraction_rules(rules_path: str) -> Dict[str, Any]:
    """Load extraction rules from YAML file."""
    try:
        rules_file = Path(rules_path)
        
        if not rules_file.exists():
            logger.warning(f"Extraction rules file not found: {rules_path}")
            return get_default_extraction_rules()
        
        with open(rules_file, 'r', encoding='utf-8') as f:
            rules = yaml.safe_load(f)
        
        logger.info(f"Loaded extraction rules from {rules_path}")
        return rules
        
    except Exception as e:
        logger.error(f"Failed to load extraction rules: {e}")
        return get_default_extraction_rules()

def get_default_extraction_rules() -> Dict[str, Any]:
    """Get default extraction rules when config file is missing."""
    return {
        "confidence_threshold": 0.7,
        "escalation_rules": {
            "fast_to_layout_threshold": 0.6,
            "layout_to_vision_threshold": 0.7
        },
        "fast_text": {
            "min_text_length": 100
        },
        "layout": {
            "enable_ocr": True,
            "ocr_engine": "auto"
        },
        "vision": {
            "dpi": 400,
            "max_pages": 5,
            "language": "eng"
        },
        "performance": {
            "max_workers": 3,
            "torch_threads": 2
        }
    }
