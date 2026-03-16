from bidbridge.data.registry import get_source_registry


def test_registry_contains_priority_sources():
    registry = get_source_registry()
    ids = {record.source_id for record in registry}
    assert "primary_dealer_statistics" in ids
    assert "treasury_auctions" in ids
    assert "investor_class_allotments" in ids


def test_registry_sorted_with_priority_first():
    registry = get_source_registry()
    assert registry[0].priority == 1
