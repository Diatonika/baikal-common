import shutil

from pathlib import Path


def is_file_exist(file_path: str | Path) -> bool:
    path = Path(file_path) if isinstance(file_path, str) else file_path
    return path.exists() and path.is_file()


def is_dir_exist(dir_path: str | Path, *, create_dir: bool = False) -> bool:
    path = Path(dir_path) if isinstance(dir_path, str) else dir_path
    if path.exists() and path.is_dir():
        return True

    if path.exists() and not path.is_dir():
        return False

    if not path.exists() and create_dir:
        path.mkdir(parents=True)

    return True


def remove_path(path: str | Path) -> bool:
    path = Path(path) if isinstance(path, str) else path
    if not path.exists():
        return False

    if path.is_file():
        path.unlink()
        return True

    if path.is_dir():
        shutil.rmtree(path)
        return True

    return False
