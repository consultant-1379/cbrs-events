import os
import sys
def adjust_python_path():
    """For testing code, adds application root package to python path.

    Package imports are relative to the python path. Python path is, by default formed as such:
    - The directory where the Python script is located
    - Python SDK default locations
    - Virtual environment directories (if in use)

    When executing tests, the directory 'cbrs_dcsa_cli' is not part of python path (only 'tests' is)
    therefore imports to production types would fail.

    To overcome this the directory 'cbrs_dcsa_cli' is added to python path before any import in testing code.

    Important: This is only required for testing code. Production code needs no adjustments in python path
    """
    path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "python")
    if path not in sys.path:
        sys.path.append(path)
