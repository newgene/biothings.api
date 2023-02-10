import pytest
from _pytest.capture import SysCapture

from biothings.tests.web import BiothingsWebTest


def capsys(session, func):
    """Mimic pytests's capsys fixture to change override Capture configuration from plugin context"""

    capman = session.config.pluginmanager.getplugin("capturemanager")
    capture_fixture = pytest.CaptureFixture[str](SysCapture, session, _ispytest=True)
    capman.set_fixture(capture_fixture)
    capture_fixture._start()

    # run function in context of disabled capture.
    func()

    capture_fixture.close()
    capman.unset_fixture()


def pytest_collection_finish(session):
    def print_base_url():
        web_test_base_url = BiothingsWebTest.get_base_url()
        if web_test_base_url:
            print(f"Web test has base url: {web_test_base_url}")
        else:
            print("Web test has no base url")

    capsys(session, print_base_url)
