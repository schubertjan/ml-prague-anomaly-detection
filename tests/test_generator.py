from mock_producer.generator import generate


def test_now_is_iso_z():
    doc = generate({"ts": {"type": "now"}})
    assert doc["ts"].endswith("Z")


def test_choice():
    doc = generate({"f": {"type": "choice", "values": ["a", "b", "c"]}})
    assert doc["f"] in ("a", "b", "c")


def test_int_range():
    for _ in range(50):
        v = generate({"n": {"type": "int", "min": 5, "max": 9}})["n"]
        assert 5 <= v <= 9


def test_static():
    assert generate({"f": {"type": "static", "value": "x"}}) == {"f": "x"}


def test_template():
    for _ in range(20):
        v = generate({"h": {"type": "template", "pattern": "host-{int:0-49}"}})["h"]
        assert v.startswith("host-")
        assert 0 <= int(v.split("-")[1]) <= 49


def test_flat_dotted_field():
    doc = generate({"process.program": {"type": "choice", "values": ["ls", "cp"]}})
    assert list(doc) == ["process.program"]
    assert doc["process.program"] in ("ls", "cp")
    assert not isinstance(doc["process.program"], dict)


def test_nested_object_rejected():
    import pytest

    with pytest.raises(ValueError, match="dotted field name"):
        generate({"nested": {"program": {"type": "choice", "values": ["ls"]}}})


def test_unknown_type_raises():
    import pytest

    with pytest.raises(ValueError, match="unknown generator"):
        generate({"f": {"type": "nope"}})
