"""Register the `network` marker so the opt-in live tests don't warn.

This conftest is local to tests/net/ so it never affects the main suite.
"""


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "network: live remote-server test; runs only with RUNLOOM_NET_TESTS=1")
