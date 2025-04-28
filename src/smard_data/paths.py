from pathlib import Path

class ProjPaths:

    current_file_path = Path(__file__)
    pkg_src_path = current_file_path.parent.parent
    project_path = current_file_path.parent.parent.parent
    data_path = project_path / "data"
    raw_data_path = data_path / "raw_data"
    processed_data_path = data_path / "processed_data"
    output_path = project_path / "output"
    
    reports_path = output_path / "reports"
    images_path = output_path / "images"
    video_story_path = output_path / "video_story"

    generation_raw_data_path = raw_data_path / "generation"
    consumption_raw_data_path = raw_data_path / "consumption"
    prices_raw_data_path = raw_data_path / "prices"
    forecasts_raw_data_path = raw_data_path / "forecasts"

    data_analysis_report_path = reports_path / "01_data_analysis"
