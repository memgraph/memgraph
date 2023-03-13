from pathlib import Path

modules = Path(__file__).resolve().parent.glob("*.py")
for f in modules:
    print(f)
    __all__ = [f.name[:-3] for f in modules if f.is_file() and not f.name == "__init__.py"]
