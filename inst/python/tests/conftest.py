"""Give each independent test its own persistent node privacy state.

A test may create multiple processes/restarts against this root; unrelated tests
must not share immutable release history or mocked node keys. No production
store, integrity check or release boundary is replaced.
"""
import pytest


@pytest.fixture(autouse=True)
def isolated_node_privacy_state(tmp_path, monkeypatch):
    secret = tmp_path / 'test-node-secret'
    secret.write_text('c7' * 32, encoding='ascii')
    secret.chmod(0o600)
    monkeypatch.setenv('DSFLOWER_NODE_SECRET_FILE', str(secret))
    for name in ('DIR', 'STORE_ID', 'K', 'MAX_ANCHORS', 'STORE_BYTES'):
        monkeypatch.delenv('DSFLOWER_NEIGHBOURHOOD_' + name, raising=False)
