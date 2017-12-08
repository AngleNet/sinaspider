from sinaspider.config import CONFIG

def test_test_config_section():
    assert CONFIG['TEST']['config']['test_dummy1'] == 1
    assert CONFIG['TEST']['config']['test_dummy2'] == "test_dummy2"
    assert CONFIG['TEST']['config']['test_dummy3'] == [1, 2, 3]
