# Ensure tests can import the application package regardless of CWD
import os
import sys

# Repo root is one directory up from the tests folder
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)
