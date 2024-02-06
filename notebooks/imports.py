import sys
from pathlib import Path

home_path = Path().absolute().parent
sys.path.insert(0, str(home_path))
