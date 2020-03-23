'''An example file that imports some of the installed modules.

Run with: `poetry run python testenv.py`
'''
import platform
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

if __name__ == "__main__":
    # If the modules can't be imported, the following print won't happen
    print("Successfully imported modules!")
    print("python", platform.python_version())
    print("pandas", pd.__version__)
