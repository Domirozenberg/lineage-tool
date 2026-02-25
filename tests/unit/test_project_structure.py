"""
Acceptance tests for Task 1.1: Project structure and development environment.
"""

import importlib
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent

REQUIRED_DIRS = [
    "app",
    "app/api",
    "app/api/v1",
    "app/connectors",
    "app/models",
    "app/db",
    "app/core",
    "tests",
    "tests/unit",
    "tests/integration",
    "scripts",
    "docs",
]

REQUIRED_FILES = [
    "requirements.txt",
    "requirements-dev.txt",
    "docker-compose.yml",
    ".pre-commit-config.yaml",
    "pyproject.toml",
    "README.md",
    "app/__init__.py",
    "app/main.py",
    "app/core/config.py",
    "app/connectors/base.py",
    "app/api/__init__.py",
    "app/api/v1/__init__.py",
    "app/models/__init__.py",
    "app/db/__init__.py",
]


def test_required_directories_exist():
    for directory in REQUIRED_DIRS:
        path = PROJECT_ROOT / directory
        assert path.is_dir(), f"Missing required directory: {directory}"


def test_required_files_exist():
    for filename in REQUIRED_FILES:
        path = PROJECT_ROOT / filename
        assert path.is_file(), f"Missing required file: {filename}"


def test_requirements_txt_has_core_dependencies():
    content = (PROJECT_ROOT / "requirements.txt").read_text()
    required_packages = ["fastapi", "uvicorn", "neo4j", "redis", "celery", "pydantic"]
    for pkg in required_packages:
        assert pkg in content, f"Missing package in requirements.txt: {pkg}"


def test_docker_compose_has_neo4j():
    content = (PROJECT_ROOT / "docker-compose.yml").read_text()
    assert "neo4j" in content
    assert "7474" in content
    assert "7687" in content


def test_docker_compose_has_redis():
    content = (PROJECT_ROOT / "docker-compose.yml").read_text()
    assert "redis" in content
    assert "6379" in content


def test_pre_commit_config_is_valid_yaml():
    import yaml

    content = (PROJECT_ROOT / ".pre-commit-config.yaml").read_text()
    config = yaml.safe_load(content)
    assert "repos" in config
    assert len(config["repos"]) > 0


def test_app_package_importable():
    spec = importlib.util.find_spec("app")
    assert spec is not None, "app package is not importable"


def test_base_connector_importable():
    from app.connectors.base import AuthMode, BaseConnector

    assert issubclass(BaseConnector, object)
    assert AuthMode.OFFLINE in AuthMode


def test_settings_importable():
    from app.core.config import Settings, settings

    assert settings.PROJECT_NAME
    assert settings.API_V1_STR == "/api/v1"
    assert settings.NEO4J_URI
    assert settings.REDIS_URL


def test_fastapi_app_importable():
    from app.main import app

    assert app is not None
    routes = [r.path for r in app.routes]
    assert "/health" in routes
