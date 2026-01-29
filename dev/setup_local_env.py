"""Setup script for local development environment."""
import os
import subprocess
import sys
from pathlib import Path

def create_env_file():
    """Create a .env file with local development values."""
    env_content = """# Local development environment variables
# These are safe default values for local testing

# Google Cloud Project Configuration (use your actual project)
GCP_PROJECT_ID=your-project-id-here
GCP_REGION=us-central1

# Pub/Sub Configuration (create these resources in your GCP project)
PUBSUB_TOPIC=projects/your-project-id-here/topics/wiki-edits

# Google Cloud Storage Configuration (create this bucket)
GCS_TEMP_LOCATION=gs://your-bucket-name/temp

# Dataflow Configuration
DATAFLOW_JOB_NAME=wiki-edits-pipeline

# BigQuery Configuration (create this dataset and table)
BIGQUERY_OUTPUT_TABLE=your-project-id-here:wiki_data.edits

# Logging Configuration
LOG_LEVEL=INFO
STRUCTURED_LOGGING=false
"""
    
    env_path = Path('.env')
    if not env_path.exists():
        with open(env_path, 'w') as f:
            f.write(env_content)
        print("✅ Created .env file with local development defaults")
        print("⚠️  Please update the values in .env with your actual GCP project details")
    else:
        print("ℹ️  .env file already exists, skipping creation")

def create_output_dir():
    """Create output directory for local pipeline results."""
    output_dir = Path('output')
    output_dir.mkdir(exist_ok=True)
    print("✅ Created output directory for local pipeline results")

def install_dependencies():
    """Install Python dependencies."""
    print("📦 Installing dependencies...")
    try:
        subprocess.run([sys.executable, '-m', 'pip', 'install', '-e', '.'], 
                      check=True, capture_output=True, text=True)
        print("✅ Dependencies installed successfully")
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install dependencies: {e}")
        print("You may need to run: pip install -e . manually")

def main():
    """Set up local development environment."""
    print("🚀 Setting up local development environment...")
    
    create_env_file()
    create_output_dir()
    
    # Ask user if they want to install dependencies
    install_deps = input("Install Python dependencies? (y/n): ").lower().strip()
    if install_deps in ['y', 'yes']:
        install_dependencies()
    
    print("\n🎉 Local environment setup complete!")
    print("\n📋 Next steps:")
    print("1. Update .env file with your actual GCP project details")
    print("2. Run tests: python dev/test_pipeline.py")
    print("3. Run local pipeline: python main.py local")
    print("4. For cloud deployment: python main.py pipeline")

if __name__ == '__main__':
    main()
