from pathlib import Path

class ProjPaths:

    current_file_path = Path(__file__)
    pkg_src_path = current_file_path.parent.parent
    project_path = current_file_path.parent.parent.parent
    data_path = project_path / "data"
    raw_data_path = data_path / "raw_data"
    processed_data_path = data_path / "processed_data"

