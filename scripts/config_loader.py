import yaml
import os

def load_config():
    """Loads the configuration from config.yml"""
    config_path = os.path.expanduser("~/Desktop/projects/dds_t11/config/config.yml")
    
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)

    return config
