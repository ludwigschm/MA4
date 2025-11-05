from core.capabilities import CapabilityRegistry, DeviceCapabilities


def test_caps_cached_and_mutable():
    registry = CapabilityRegistry()
    caps = registry.get("devA")
    assert caps.frame_name_supported is False

    updated = DeviceCapabilities(frame_name_supported=True)
    registry.set("devA", updated)
    cached = registry.get("devA")
    assert cached.frame_name_supported is True
    assert registry.get("devB").frame_name_supported is False
