"""
Import Redirector - Hot-replace module without modifying source code.

This module provides a meta path finder that intercepts import statements
and redirects them to alternative modules as specified in a mapping.
Useful for replacing pyspark with polarspark
"""

import sys
import importlib
from importlib.abc import MetaPathFinder
from importlib.machinery import ModuleSpec
from typing import Optional, Dict, Any


class ImportRedirector(MetaPathFinder):
    """
    A meta path finder that redirects imports from one module to another.
    
    This allows transparent replacement of modules at import time without
    modifying the source code that imports them.
    """
    
    def __init__(self, mapping: Optional[Dict[str, str]] = None):
        """
        Initialize the import redirector.
        
        Args:
            mapping: Dictionary mapping original module names to replacement module names.
                     For example: {'pyspark': 'polarspark', 'pyspark.sql': 'polarspark.sql'}
        """
        self.mapping = mapping or {}
    
    def find_spec(self, fullname: str, path: Any = None, target: Any = None) -> Optional[ModuleSpec]:
        """
        Find a module spec, redirecting to the mapped module if specified.
        
        Args:
            fullname: The fully qualified name of the module to import
            path: The path to search (for submodules)
            target: The target module (for reloading)
            
        Returns:
            ModuleSpec if a redirect is found, None otherwise
        """
        # If module is already cached in sys.modules, skip the redirect
        # This prevents redundant imports and preserves the cached module
        if fullname in sys.modules:
            return None
        
        # Check for direct mapping
        if fullname in self.mapping:
            redirect_name = self.mapping[fullname]
            try:
                # Import the redirect target
                redirect_module = importlib.import_module(redirect_name)
                # Cache the module in sys.modules under the original name
                sys.modules[fullname] = redirect_module
                # Return a spec that will use the redirected module
                return ModuleSpec(fullname, None, is_package=hasattr(redirect_module, '__path__'))
            except ImportError:
                return None
        
        # Check for submodule redirects (e.g., pyspark.sql -> polarspark.sql)
        for original, replacement in self.mapping.items():
            if fullname.startswith(original + '.'):
                # Calculate the redirected module name
                redirect_name = fullname.replace(original, replacement, 1)
                try:
                    # Import the redirect target
                    redirect_module = importlib.import_module(redirect_name)
                    # Cache the module in sys.modules under the original name
                    sys.modules[fullname] = redirect_module
                    return ModuleSpec(fullname, None, is_package=hasattr(redirect_module, '__path__'))
                except ImportError:
                    return None
        
        # No redirect found, let other finders handle it
        return None
    
    def install(self) -> None:
        """Install this redirector into sys.meta_path."""
        if self not in sys.meta_path:
            sys.meta_path.insert(0, self)
    
    def uninstall(self) -> None:
        """Remove this redirector from sys.meta_path."""
        if self in sys.meta_path:
            sys.meta_path.remove(self)


def activate_polarspark() -> ImportRedirector:
    """
    Convenience function to create and install an import redirector.
    This redirector maps 'pyspark' imports to 'polarspark'.
    Returns:
        The installed ImportRedirector instance
    
    Example:
        >>> redirector = activate_polarspark()
        >>> import pyspark  # Will actually import polarspark
    """
    mapping = {'pyspark': 'polarspark'}
    
    redirector = ImportRedirector(mapping)
    redirector.install()
    return redirector


def deactivate_polarspark(redirector: ImportRedirector) -> None:
    """
    Uninstall an import redirector.
    
    Args:
        redirector: The ImportRedirector instance to uninstall
    """
    redirector.uninstall()