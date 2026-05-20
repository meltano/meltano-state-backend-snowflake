from importlib.metadata import version


def pytest_report_header() -> list[str]:
    """Add Meltano version to test report header."""
    return [f"Meltano v{version('meltano')}"]
