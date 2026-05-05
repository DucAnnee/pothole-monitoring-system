import ast
import importlib.util
import json
import os
import sys
from contextlib import contextmanager
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def repo_root() -> Path:
    return REPO_ROOT


@contextmanager
def prepend_syspath(path: Path):
    path_str = str(path)
    sys.path.insert(0, path_str)
    try:
        yield
    finally:
        try:
            sys.path.remove(path_str)
        except ValueError:
            pass


def load_module_from_path(module_name: str, path: Path, import_path: Path | None = None):
    import_path = import_path or path.parent
    local_module_names = [
        "config_loader",
        "data_models",
        "uploader",
        "segmentation",
        "bev_processor",
        "config",
        "pothole_area_estimator",
        "latency_tracker",
        "device_state",
        "gps_provider",
    ]
    saved_modules = {
        name: sys.modules.pop(name)
        for name in local_module_names
        if name in sys.modules
    }
    with prepend_syspath(import_path):
        try:
            spec = importlib.util.spec_from_file_location(module_name, path)
            module = importlib.util.module_from_spec(spec)
            assert spec and spec.loader
            spec.loader.exec_module(module)
            return module
        finally:
            for name in local_module_names:
                sys.modules.pop(name, None)
            sys.modules.update(saved_modules)


def extract_constant(path: Path, name: str) -> str:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == name:
                    value = ast.literal_eval(node.value)
                    assert isinstance(value, str)
                    return value
    raise AssertionError(f"{name} not found in {path}")


def avro_fields(schema_str: str) -> dict:
    schema = json.loads(schema_str)
    return {field["name"]: field for field in schema["fields"]}


@pytest.fixture
def load_module():
    return load_module_from_path


@pytest.fixture
def schema_constant():
    return extract_constant
