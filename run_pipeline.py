import json
import sys
import subprocess
from pathlib import Path
import os

def run_notebook(path):
    print(f"\n========================================\nRunning notebook: {path}\n========================================")
    abs_path = Path(path).resolve()
    notebook_dir = abs_path.parent
    old_cwd = os.getcwd()
    
    with open(abs_path, "r", encoding="utf-8") as f:
        nb = json.load(f)
    
    import sys
    namespace = sys.modules['__main__'].__dict__
    
    try:
        os.chdir(notebook_dir)
        for i, cell in enumerate(nb.get("cells", [])):
            if cell.get("cell_type") == "code":
                source = "".join(cell.get("source", []))
                if not source.strip():
                    continue
                print(f"\n--- Executing cell {i+1} ---")
                try:
                    exec(compile(source, f"{abs_path}_cell_{i+1}", "exec"), namespace)
                except Exception as e:
                    print(f"Error executing cell {i+1}: {e}")
                    import traceback
                    traceback.print_exc()
                    raise e
    finally:
        os.chdir(old_cwd)

if __name__ == "__main__":
    venv_python = str(Path(__file__).parent / "venv" / "bin" / "python")
    
    # 1. Run ingestion
    print("Running ingestion.py...")
    subprocess.run([venv_python, "src/ingestion.py"], check=True)
    
    # 2. Run quality
    print("Running quality.py...")
    subprocess.run([venv_python, "src/quality.py"], check=True)
    
    # 3. Run Silver Pipeline
    run_notebook("notebooks/silver_pipeline.ipynb")
    
    # 4. Run Gold Pipeline
    run_notebook("notebooks/gold_pipeline.ipynb")
    
    # 5. Run ML Models Training & Comparison
    run_notebook("notebooks/ml_models.ipynb")
    
    print("\nAll pipelines executed successfully!")
