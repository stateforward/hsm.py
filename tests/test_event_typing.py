import hsm


def test_event_data_type_parameter_is_covariant() -> None:
    (data_type,) = hsm.Event.__parameters__

    assert data_type.__covariant__
