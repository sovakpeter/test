#!/usr/bin/env python3
"""
Python Cache Cleaner
Recursively removes __pycache__ folders and .pyc files from the current directory and all subdirectories.
"""

import os
import shutil
import sys
from pathlib import Path

def clean_python_cache(root_path='.'):
    """Remove all Python cache folders and .pyc files recursively."""
    root = Path(root_path).resolve()
    removed_folders = 0
    removed_files = 0
    
    print(f"Cleaning Python cache in: {root}")
    print("-" * 50)
    
    # Walk through all directories
    for current_path in root.rglob('*'):
        # Skip anything inside .venv folders
        if '.venv' in current_path.parts:
            continue
        # Remove __pycache__ folders
        if current_path.is_dir() and current_path.name == '__pycache__':
            try:
                shutil.rmtree(current_path)
                print(f"Removed folder: {current_path.relative_to(root)}")
                removed_folders += 1
            except Exception as e:
                print(f"Error removing {current_path}: {e}")
        
        # Remove .pyc files
        elif current_path.is_file() and current_path.suffix == '.pyc':
            try:
                current_path.unlink()
                print(f"Removed file: {current_path.relative_to(root)}")
                removed_files += 1
            except Exception as e:
                print(f"Error removing {current_path}: {e}")
    
    print("-" * 50)
    print(f"Cleanup complete!")
    print(f"Removed {removed_folders} __pycache__ folders")
    print(f"Removed {removed_files} .pyc files")

if __name__ == "__main__":
    try:
        clean_python_cache()
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)