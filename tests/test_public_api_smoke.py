import hsm


def test_package_import_exports_public_api() -> None:
    assert all(hasattr(hsm, name) for name in hsm.__all__)
    assert "Define" in hsm.__all__
    assert hsm.Define is not None
    assert hsm.Config is not None


def test_config_accepts_documented_pascal_case_options() -> None:
    clock = hsm.DefaultClock()

    config = hsm.Config(ID="machine-1", Name="RuntimeName", Data={"ok": True}, Clock=clock)

    assert config.ID == "machine-1"
    assert config.Name == "RuntimeName"
    assert config.Data == {"ok": True}
    assert config.Clock is clock


def test_define_populates_model_members() -> None:
    model = hsm.Define(
        "Smoke",
        hsm.Initial(hsm.Target("idle")),
        hsm.State("idle"),
    )

    assert model.qualified_name == "/Smoke"
    assert "/Smoke" in model.members
    assert "/Smoke/idle" in model.members
    assert "/Smoke/.initial" in model.members
    assert model.initial == "/Smoke/.initial"
