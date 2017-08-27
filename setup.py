import sys
from cx_Freeze import setup, Executable

# Dependencies are automatically detected, but it might need fine tuning.
build_exe_options = {"packages": ["os","idna","dbm"], "excludes": ["tkinter"]}

# GUI applications require a different base on Windows (the default is for a
# console application).
base = None
# if sys.platform == "win32":
#    base = "Win32GUI"

setup(  name = "S3_backup",
        version = "0.1",
        description = "S3 backup solution",
        options = {"build_exe": build_exe_options},
        executables = [Executable("S3_backup.py", base=base)])