"""
Pandas utilities for koheesio core

This module provides pandas import utilities that are spark-aware but spark-independent.
The pandas module can work standalone or with optional PySpark version validation.
"""

from types import ModuleType


def import_pandas_with_version_check() -> ModuleType:
    """
    Import pandas with optional PySpark version compatibility check.
    
    This function imports pandas and optionally validates version compatibility with PySpark
    if PySpark is available. If PySpark is not available, it simply returns pandas.
    
    This allows pandas functionality to work independently while being spark-aware when needed.
    
    Returns
    -------
    ModuleType
        The pandas module
        
    Raises
    ------
    ImportError
        If pandas is not installed or version compatibility check fails
    """
    try:
        import pandas as pd
    except ImportError as e:
        raise ImportError("Pandas module is not installed. Install it with 'pip install pandas'") from e

    # Optional: Check PySpark compatibility if PySpark is available
    try:
        # Import spark utils only if available (no hard dependency)
        from koheesio.spark.utils.common import get_spark_minor_version
    except ImportError:
        # PySpark not available or spark utils not available - that's fine
        # Pandas can work independently
        pass
    else:
        # PySpark is available, so validate version compatibility
        pyspark_version = get_spark_minor_version()
        pandas_version = pd.__version__
        
        # Validate version compatibility
        if (pyspark_version < 3.4 and pandas_version >= "2") or (pyspark_version >= 3.4 and pandas_version < "2"):
            raise ImportError(
                f"Pandas version {pandas_version} is incompatible with PySpark {pyspark_version}. "
                f"For PySpark {pyspark_version}, "
                f"please install Pandas version {'< 2' if pyspark_version < 3.4 else '>= 2'}"
            )
    
    return pd


def import_pandas() -> ModuleType:
    """
    Simple pandas import without version checking.
    
    Use this when you want pandas without any PySpark compatibility validation.
    
    Returns
    -------
    ModuleType
        The pandas module
        
    Raises
    ------
    ImportError
        If pandas is not installed
    """
    try:
        import pandas as pd
        return pd
    except ImportError as e:
        raise ImportError("Pandas module is not installed. Install it with 'pip install pandas'") from e
