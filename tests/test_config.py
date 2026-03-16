from bidbridge.config import load_sources_config, load_study_config


def test_study_config_loads():
    config = load_study_config()
    assert config["project"]["name"] == "bidbridge"


def test_sources_config_loads():
    config = load_sources_config()
    assert "primary_dealer_statistics" in config["sources"]
    assert "treasury_auctions" in config["sources"]
