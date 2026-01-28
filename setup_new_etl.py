#!/usr/bin/env python3
"""
Interactive script to set up a new ETL job folder in the data-processing repository.

This script asks a series of questions and then creates all necessary files and folders
for a new ETL job, including:
- Folder structure (data/, data_orig/, change_tracking/)
- Dockerfile
- pyproject.toml
- etl.py with common setup
- Optional README.md
"""

import sys
from pathlib import Path


def ask_yes_no(question: str, default: bool = True) -> bool:
    """Ask a yes/no question and return a boolean."""
    default_str = "Y/n" if default else "y/N"
    response = input(f"{question} [{default_str}]: ").strip().lower()
    if not response:
        return default
    return response in ("y", "yes")


def ask_string(question: str, default: str = "") -> str:
    """Ask for a string input with optional default."""
    if default:
        response = input(f"{question} [{default}]: ").strip()
        return response if response else default
    while True:
        response = input(f"{question}: ").strip()
        if response:
            return response
        print("This field is required. Please provide a value.")


def create_folder_structure(folder_path: Path, include_data_orig: bool, include_change_tracking: bool):
    """Create the folder structure for the ETL job."""
    folder_path.mkdir(parents=True, exist_ok=True)

    # Always create data/ folder
    (folder_path / "data").mkdir(exist_ok=True)
    (folder_path / "data" / ".gitkeep").touch()

    if include_data_orig:
        (folder_path / "data_orig").mkdir(exist_ok=True)
        (folder_path / "data_orig" / ".gitkeep").touch()

    if include_change_tracking:
        (folder_path / "change_tracking").mkdir(exist_ok=True)
        (folder_path / "change_tracking" / ".gitkeep").touch()


def create_dockerfile(folder_path: Path):
    """Create the Dockerfile for the ETL job."""
    dockerfile_content = """# Base image from the github.com/opendatabs/data-processing repo
FROM ghcr.io/opendatabs/data-processing/base:latest

COPY uv.lock pyproject.toml /code/
RUN uv sync --frozen

COPY . /code/

CMD ["uv", "run", "-m", "etl"]
"""
    (folder_path / "Dockerfile").write_text(dockerfile_content)


def create_pyproject_toml(folder_path: Path, project_name: str, include_common: bool, include_pandas: bool):
    """Create the pyproject.toml file with dependencies."""
    dependencies = []

    if include_common:
        dependencies.append('    "common",')

    if include_pandas:
        dependencies.append('    "pandas>=2.2.3",')

    # Add python-dotenv as it's commonly used
    dependencies.append('    "python-dotenv>=1.1.0",')

    deps_str = "\n".join(dependencies) if dependencies else "    # Add your dependencies here"

    # Get the latest common rev from an existing project (we'll use a placeholder)
    # Users should update this with the correct rev
    pyproject_content = f"""[project]
name = "{project_name}"
version = "0.1.0"
description = "Add your description here"
readme = "README.md"
requires-python = ">=3.12"
dependencies = [
{deps_str}
]

[tool.uv.sources]
"""

    if include_common:
        pyproject_content += 'common = { git = "https://github.com/opendatabs/common", rev = "dfd6ec3541e8506a906b8a0e51f6cf8ac717077e" }\n'

    (folder_path / "pyproject.toml").write_text(pyproject_content)


def create_etl_py(folder_path: Path, include_common: bool, include_pandas: bool, include_change_tracking: bool):
    """Create the etl.py file with common setup."""
    imports = ["import logging", "import os"]

    if include_common:
        imports.append("import common")
        if include_change_tracking:
            imports.append("import common.change_tracking as ct")

    if include_pandas:
        imports.append("import pandas as pd")

    imports.append("from dotenv import load_dotenv")

    imports_str = "\n".join(imports)

    load_dotenv_line = "load_dotenv()"  # Always include dotenv

    main_function = """def main():
    \"\"\"Main ETL function.\"\"\"
    # TODO: Implement your ETL logic here
    logging.info("ETL job started")
    
    # Example: Read from data_orig if it exists
    # if os.path.exists("data_orig"):
    #     logging.info("Reading source data...")
    #     # Add your data reading logic here
    
    # Example: Process and write to data/
    # logging.info("Processing data...")
    # # Add your processing logic here
    
    # logging.info("Writing processed data...")
    # # Add your writing logic here
    
    logging.info("ETL job completed")
"""

    main_block = """if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful.")
"""

    etl_content = f"""{imports_str}

{load_dotenv_line}


{main_function}


{main_block}
"""

    (folder_path / "etl.py").write_text(etl_content)


def create_readme(folder_path: Path, data_owner: str, dataset_name: str):
    """Create a README.md file for the ETL job."""
    readme_content = f"""# {dataset_name}

ETL job for processing {dataset_name} data.

## Data Owner
{data_owner}

## Description
Add a description of what this ETL job does and what data it processes.

## Source Data
Describe where the source data comes from and how it's provided.

## Output
Describe what data files are generated and where they are published.
"""
    (folder_path / "README.md").write_text(readme_content)


def create_python_version(folder_path: Path):
    """Create .python-version file."""
    (folder_path / ".python-version").write_text("3.12\n")


def main():
    """Main function to run the interactive setup."""
    print("=" * 60)
    print("ETL Job Setup Script")
    print("=" * 60)
    print()

    # Get repository root (assuming script is run from repo root)
    repo_root = Path.cwd()
    if not (repo_root / ".github").exists():
        print("Error: This script must be run from the repository root directory.")
        sys.exit(1)

    # Ask questions
    print("Please answer the following questions to set up your new ETL job:")
    print()

    data_owner = ask_string("Data owner (e.g., AUE, GVA, StatA): ")
    dataset_name = ask_string("Dataset name (e.g., umweltlabor, geodatenshop): ")

    # Generate folder name
    folder_name = f"{data_owner.lower()}_{dataset_name.lower()}".replace(" ", "_")
    print(f"\nGenerated folder name: {folder_name}")

    if not ask_yes_no("Use this folder name?", default=True):
        folder_name = ask_string("Enter folder name: ")

    folder_path = repo_root / folder_name

    # Check if folder already exists
    if folder_path.exists():
        print(f"\nError: Folder '{folder_name}' already exists!")
        if not ask_yes_no("Continue anyway? (will overwrite existing files)", default=False):
            sys.exit(1)

    # Ask about dependencies and structure
    print()
    include_common = ask_yes_no("Include 'common' library?", default=True)
    include_pandas = ask_yes_no("Include 'pandas'?", default=True)
    include_data_orig = ask_yes_no("Create 'data_orig/' folder?", default=True)
    include_change_tracking = ask_yes_no("Create 'change_tracking/' folder?", default=True)
    include_readme = ask_yes_no("Create README.md?", default=True)

    print()
    print("=" * 60)
    print("Creating ETL job structure...")
    print("=" * 60)

    # Create folder structure
    create_folder_structure(folder_path, include_data_orig, include_change_tracking)
    print(f"✓ Created folder structure in {folder_name}/")

    # Create files
    create_dockerfile(folder_path)
    print("✓ Created Dockerfile")

    project_name = folder_name.replace("_", "-")
    create_pyproject_toml(folder_path, project_name, include_common, include_pandas)
    print("✓ Created pyproject.toml")

    create_etl_py(folder_path, include_common, include_pandas, include_change_tracking)
    print("✓ Created etl.py")

    create_python_version(folder_path)
    print("✓ Created .python-version")

    if include_readme:
        create_readme(folder_path, data_owner, dataset_name)
        print("✓ Created README.md")

    print()
    print("=" * 60)
    print("Setup complete!")
    print("=" * 60)
    print()
    print("Next steps:")
    print(f"1. Navigate to the folder: cd {folder_name}")
    print("2. Install dependencies: uv sync")
    print("3. Implement your ETL logic in etl.py")
    print("4. Test locally: uv run -m etl")
    print()
    print("⚠️  IMPORTANT: Add the new folder to the GitHub workflow!")
    print("   Edit .github/workflows/docker_build.yaml and add:")
    print(f"   {folder_name}:")
    print("     - 'Dockerfile'")
    print(f"     - '{folder_name}/**'")
    print()
    print("⚠️  After first push, set Docker image visibility to Public:")
    print("   1. Go to repository Packages section on GitHub")
    print("   2. Find the image for your ETL job")
    print("   3. Change visibility from Private to Public")


if __name__ == "__main__":
    main()
