from bidbridge.paths import ROOT, ensure_project_directories


def test_repo_root_has_configs():
    assert (ROOT / "configs").exists()
    assert (ROOT / "pyproject.toml").exists()


def test_directories_created():
    directories = ensure_project_directories()
    assert directories
    for directory in directories:
        assert directory.exists()
