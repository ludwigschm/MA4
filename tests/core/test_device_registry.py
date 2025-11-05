from core.device_registry import DeviceRegistry


def test_confirm_and_resolve_device_id_with_single_warning_on_mismatch(caplog):
    caplog.set_level("WARNING")
    registry = DeviceRegistry()
    endpoint = "127.0.0.1:8080"

    registry.confirm(endpoint, "devA")
    assert registry.resolve(endpoint) == "devA"

    registry.confirm(endpoint, "devB")
    assert registry.resolve(endpoint) == "devB"
    warnings = [record for record in caplog.records if "device_id mismatch" in record.message]
    assert len(warnings) == 1

    registry.confirm(endpoint, "devC")
    warnings = [record for record in caplog.records if "device_id mismatch" in record.message]
    assert len(warnings) == 1
