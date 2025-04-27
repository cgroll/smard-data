import subprocess
import sys
import click
import os
from smard_data.paths import ProjPaths

def run_command(command):
    """Helper function to run a shell command and check for errors."""
    print(f"Running: {' '.join(command)}")
    result = subprocess.run(command, capture_output=True, text=True, check=False) # check=False to handle error manually
    if result.returncode != 0:
        print(f"Error running command: {' '.join(command)}", file=sys.stderr)
        print(f"Stderr: {result.stderr}", file=sys.stderr)
        print(f"Stdout: {result.stdout}", file=sys.stderr)
        sys.exit(f"Command failed with exit code {result.returncode}")
    print(result.stdout)
    return result

def ensure_output_dir():
    """Ensure the output/reports directory exists"""
    ProjPaths.reports_path.mkdir(parents=True, exist_ok=True)

@click.command()
@click.argument('python_script', type=click.Path(exists=True))
def generate_report(python_script):
    """Generate HTML report from Python script using Jupyter"""
    ensure_output_dir()
    
    # Derive filenames
    script_basename = os.path.splitext(os.path.basename(python_script))[0]
    notebook_file = f"{script_basename}.ipynb"
    html_output = f"{script_basename}.html"
    markdown_output = f"{script_basename}.md"
    pdf_output = f"{script_basename}.pdf"
    
    reports_path = ProjPaths.reports_path

    print(f"STEP 1: Converting {python_script} to {reports_path}/{notebook_file} using jupytext...")
    run_command(["jupytext", "--to", "notebook", "--output", str(reports_path / notebook_file), python_script])

    print(f"\nSTEP 2: Executing {reports_path}/{notebook_file} and saving outputs (in place)...")
    run_command([
        "jupyter", "nbconvert",
        "--to", "notebook",
        "--execute",
        "--inplace",
        str(reports_path / notebook_file)
    ])

    print(f"\nSTEP 3: HTML Exporting executed {reports_path}/{notebook_file} to {reports_path}/{html_output}...")
    run_command([
        "jupyter", "nbconvert",
        "--to", "html",
        "--template", "basic",
        "--no-input",
        "--output", html_output,
        str(reports_path / notebook_file)
    ])

    print(f"\nSTEP 4: Markdown Exporting executed {reports_path}/{notebook_file} to {reports_path}/{markdown_output}...")
    run_command([
        "jupyter", "nbconvert",
        "--to", "markdown",
        "--output", markdown_output,
        "--no-input",
        str(reports_path / notebook_file)
    ])
    
    print(f"\nSTEP 5: PDF Exporting executed {reports_path}/{notebook_file} to {reports_path}/{pdf_output}...")
    run_command([
        "jupyter", "nbconvert",
        "--to", "pdf",
        "--output", pdf_output,
        "--no-input",
        str(reports_path / notebook_file)
    ])

    # Clean up intermediate notebook file
    (reports_path / notebook_file).unlink()

    print("\n-------------------------------------")
    print("Report generation complete!")
    print(f"Input Python script: {python_script}")
    print(f"Output HTML report: {reports_path}/{html_output}")
    print(f"Output Markdown report: {reports_path}/{markdown_output}")
    print("-------------------------------------")

if __name__ == '__main__':
    generate_report()