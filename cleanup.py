import subprocess
import shutil
import sys

def delete_temp_dir(TMP_DIR:str) -> None:
    """
    Deletes a temporary directory.
    """
    shutil.rmtree(TMP_DIR)
    subprocess.run(f"rm -rf {TMP_DIR}", shell=True)

