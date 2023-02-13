from pathlib import Path

modules = Path().absolute().glob("workload/*.py")
for f in modules:
    __all__ = [f.name[:-3] for f in modules if f.is_file() and not f.name == "__init__.py"]
