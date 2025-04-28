import subprocess
import sys
from pathlib import Path
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

def ensure_output_dir(report_dir):
    """Ensure the output directory exists"""
    report_dir.mkdir(parents=True, exist_ok=True)

def generate_report(python_script):
    """Generate HTML report from Python script using Jupyter"""
    script_path = Path(python_script)
    if not script_path.exists():
        sys.exit(f"Analysis script not found: {python_script}")
        
    # Create report subdirectory based on script name
    script_basename = script_path.stem
    report_dir = ProjPaths().reports_path / script_basename
    ensure_output_dir(report_dir)
    
    # Define output files
    notebook_file = report_dir / f"{script_basename}.ipynb"
    html_output = report_dir / f"{script_basename}.html"
    markdown_output = report_dir / f"{script_basename}.md"
    pdf_output = report_dir / f"{script_basename}.pdf"

    print(f"STEP 1: Converting {python_script} to {notebook_file} using jupytext...")
    run_command(["jupytext", "--to", "notebook", "--output", str(notebook_file), str(script_path)])

    print(f"\nSTEP 2: Executing {notebook_file} and saving outputs (in place)...")
    run_command([
        "jupyter", "nbconvert",
        "--to", "notebook",
        "--execute",
        "--inplace",
        str(notebook_file)
    ])

    print(f"\nSTEP 3: HTML Exporting executed {notebook_file} to {html_output}...")
    run_command([
        "jupyter", "nbconvert",
        "--to", "html",
        "--template", "basic",
        "--no-input",
        "--output", str(html_output),
        str(notebook_file)
    ])

    print(f"\nSTEP 4: Markdown Exporting executed {notebook_file} to {markdown_output}...")
    run_command([
        "jupyter", "nbconvert",
        "--to", "markdown",
        "--output", str(markdown_output),
        "--no-input",
        str(notebook_file)
    ])
    
    print(f"\nSTEP 5: PDF Exporting executed {notebook_file} to {pdf_output}...")
    run_command([
        "jupyter", "nbconvert",
        "--to", "pdf",
        "--output", str(pdf_output),
        "--no-input",
        str(notebook_file)
    ])

    # Clean up intermediate notebook file
    notebook_file.unlink()

    print("\n-------------------------------------")
    print("Report generation complete!")
    print(f"Input Python script: {python_script}")
    print(f"Output directory: {report_dir}")
    print("Generated files:")
    print(f"- HTML report: {html_output}")
    print(f"- Markdown report: {markdown_output}")
    print(f"- PDF report: {pdf_output}")
    print("-------------------------------------")

if __name__ == '__main__':
    if len(sys.argv) != 2:
        sys.exit("Usage: python generate_report.py <analysis_script.py>")
    generate_report(sys.argv[1])