#!/usr/bin/env python3
"""Generate requirements.txt from pyproject.toml for Dataflow deployment."""
import toml
import sys
from pathlib import Path

def generate_requirements():
    """Generate requirements.txt from pyproject.toml dependencies."""
    pyproject_path = Path('pyproject.toml')
    
    if not pyproject_path.exists():
        print("❌ pyproject.toml not found")
        sys.exit(1)
    
    try:
        with open(pyproject_path, 'r') as f:
            pyproject_data = toml.load(f)
        
        dependencies = pyproject_data.get('project', {}).get('dependencies', [])
        
        if not dependencies:
            print("❌ No dependencies found in pyproject.toml")
            sys.exit(1)
        
        # Write requirements.txt
        with open('requirements.txt', 'w') as f:
            f.write("# Auto-generated from pyproject.toml - do not edit manually\n")
            for dep in dependencies:
                f.write(f"{dep}\n")
        
        print(f"✅ Generated requirements.txt with {len(dependencies)} dependencies")
        print("📋 Contents:")
        for dep in dependencies:
            print(f"  - {dep}")
            
    except Exception as e:
        print(f"❌ Failed to generate requirements.txt: {e}")
        sys.exit(1)

if __name__ == '__main__':
    generate_requirements()