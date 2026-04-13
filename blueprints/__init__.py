# blueprints/__init__.py

# This file can be empty, or you can use it to initialize the package.
# You can also import blueprints here for easier access.

from .systems import systems_bp
from .subsystems import subsystems_bp
from .testpads import testpads_bp
from .units import units_bp
from .aggregations_original import aggregations_bp
from .alignments import alignments_bp
from .durations import durations_bp
from .labels import labels_bp
from .manufacturers import manufacturers_bp
from .module_models import module_models_bp
from .modules import modules_bp
from .projects import projects_bp
from .sites import sites_bp
from .loads import loads_bp

# Optionally, you can expose all blueprints in a single import
__all__ = [
    'systems_bp',
    'subsystems_bp',
    'testpads_bp',
    'units_bp',
    'aggregations_bp',
    'alignments_bp',
    'durations_bp',
    'labels_bp',
    'manufacturers_bp',
    'module_models_bp',
    'modules_bp',
    'projects_bp',
    'sites_bp',
    'loads_bp',
]