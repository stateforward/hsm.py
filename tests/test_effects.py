"""
Test transition effects and behaviors
Tests effect execution, ordering, and error handling
"""

import pytest
import asyncio
import hsm


class EffectInstance(hsm.Instance):
    """Test instance implementation for effect testing"""
    
    def __init__(self):
        super().__init__()
        self.log = []
        self.data = {}

    def log_action(self, action: str):
        """Log an action for test verification"""
        self.log.append(action)


@pytest.mark.asyncio
async def test_single_effect_execution():
    """Test single effect execution on transitions"""
    instance = EffectInstance()

    def test_effect(ctx: hsm.Context, inst: EffectInstance, event: hsm.Event) -> None:
        inst.log_action('effect-executed')
        inst.data['effect_called'] = True

    model = hsm.define('SingleEffectMachine',
        hsm.initial(hsm.target('state1')),
        hsm.state('state1',
            hsm.transition(
                hsm.on('trigger'),
                hsm.target('../state2'),
                hsm.effect(test_effect)
            )
        ),
        hsm.state('state2')
    )

    ctx = hsm.hsm.context.new_context()
    await hsm.started(ctx, instance, model)

    await instance.dispatch(ctx, hsm.Event(name='trigger'))

    assert 'effect-executed' in instance.log
    assert instance.data.get('effect_called') is True
    assert instance.state() == '/SingleEffectMachine/state2'

    await instance.stop(ctx)


@pytest.mark.asyncio
async def test_multiple_effects_execution_order():
    """Test multiple effects execution in correct order"""
    instance = EffectInstance()

    def effect1(ctx: hsm.Context, inst: EffectInstance, event: hsm.Event) -> None:
        inst.log_action('effect1')

    def effect2(ctx: hsm.Context, inst: EffectInstance, event: hsm.Event) -> None:
        inst.log_action('effect2')

    def effect3(ctx: hsm.Context, inst: EffectInstance, event: hsm.Event) -> None:
        inst.log_action('effect3')

    model = hsm.define('MultipleEffectMachine',
        hsm.initial(hsm.target('state1')),
        hsm.state('state1',
            hsm.transition(
                hsm.on('trigger'),
                hsm.target('../state2'),
                hsm.effect(effect1),
                hsm.effect(effect2),
                hsm.effect(effect3)
            )
        ),
        hsm.state('state2')
    )

    ctx = hsm.hsm.context.new_context()
    await hsm.started(ctx, instance, model)

    await instance.dispatch(ctx, hsm.Event(name='trigger'))

    # Effects should be executed in order
    expected_order = ['effect1', 'effect2', 'effect3']
    assert instance.log == expected_order
    assert instance.state() == '/MultipleEffectMachine/state2'

    await instance.stop(ctx)


@pytest.mark.asyncio
async def test_effect_with_event_access():
    """Test effects with access to event information"""
    instance = EffectInstance()

    def event_effect(ctx: hsm.Context, inst: EffectInstance, event: hsm.Event) -> None:
        inst.log_action(f'effect-for-{event.name}')
        inst.data['event_name'] = event.name
        inst.data['event_kind'] = event.kind

    model = hsm.define('EventEffectMachine',
        hsm.initial(hsm.target('waiting')),
        hsm.state('waiting',
            hsm.transition(
                hsm.on('testEvent'),
                hsm.target('../done'),
                hsm.effect(event_effect)
            )
        ),
        hsm.state('done')
    )

    ctx = hsm.hsm.context.new_context()
    await hsm.started(ctx, instance, model)

    await instance.dispatch(ctx, hsm.Event(name='testEvent'))

    assert 'effect-for-testEvent' in instance.log
    assert instance.data.get('event_name') == 'testEvent'
    assert instance.data.get('event_kind') == hsm.EventKind

    await instance.stop(ctx)


def test_async_effects_are_rejected_at_build_time():
    async def async_effect(
        ctx: hsm.Context, inst: EffectInstance, event: hsm.Event
    ) -> None:
        inst.log_action("async-effect-completed")

    with pytest.raises(
        hsm.ValidationError, match="effect must be a synchronous function"
    ):
        hsm.Define(
            "AsyncEffectMachine",
            hsm.Initial(hsm.Target("waiting")),
            hsm.State(
                "waiting",
                hsm.Transition(
                    hsm.On("process"),
                    hsm.Target("../done"),
                    hsm.Effect(async_effect),
                ),
            ),
            hsm.State("done"),
        )


@pytest.mark.asyncio
async def test_effects_on_internal_transitions():
    """Test effects on internal transitions"""
    instance = EffectInstance()

    def internal_effect(ctx: hsm.Context, inst: EffectInstance, event: hsm.Event) -> None:
        inst.log_action('internal-effect')
        inst.data['internal_count'] = inst.data.get('internal_count', 0) + 1

    model = hsm.define('InternalEffectMachine',
        hsm.initial(hsm.target('active')),
        hsm.state('active',
            hsm.transition(
                hsm.on('internal'),
                # No target = internal transition
                hsm.effect(internal_effect)
            )
        )
    )

    ctx = hsm.hsm.context.new_context()
    await hsm.started(ctx, instance, model)

    # Internal transition should execute effect but not change state
    await instance.dispatch(ctx, hsm.Event(name='internal'))
    assert 'internal-effect' in instance.log
    assert instance.data.get('internal_count') == 1
    assert instance.state() == '/InternalEffectMachine/active'

    # Can be triggered multiple times
    instance.log.clear()
    await instance.dispatch(ctx, hsm.Event(name='internal'))
    assert 'internal-effect' in instance.log
    assert instance.data.get('internal_count') == 2

    await instance.stop(ctx)


@pytest.mark.asyncio
async def test_effects_with_state_mutation():
    """Test effects that modify instance state"""
    instance = EffectInstance()

    def state_effect(ctx: hsm.Context, inst: EffectInstance, event: hsm.Event) -> None:
        inst.log_action('modifying-state')
        inst.data['modified'] = True
        inst.data['counter'] = inst.data.get('counter', 0) + 1

    def read_effect(ctx: hsm.Context, inst: EffectInstance, event: hsm.Event) -> None:
        inst.log_action(f'counter-is-{inst.data.get("counter", 0)}')

    model = hsm.define('StateMutationMachine',
        hsm.initial(hsm.target('state1')),
        hsm.state('state1',
            hsm.transition(
                hsm.on('modify'),
                hsm.target('../state2'),
                hsm.effect(state_effect)
            )
        ),
        hsm.state('state2',
            hsm.transition(
                hsm.on('read'),
                hsm.target('../state3'),
                hsm.effect(read_effect)
            )
        ),
        hsm.state('state3')
    )

    ctx = hsm.hsm.context.new_context()
    await hsm.started(ctx, instance, model)

    await instance.dispatch(ctx, hsm.Event(name='modify'))
    assert 'modifying-state' in instance.log
    assert instance.data.get('modified') is True
    assert instance.data.get('counter') == 1

    await instance.dispatch(ctx, hsm.Event(name='read'))
    assert 'counter-is-1' in instance.log

    await instance.stop(ctx)


@pytest.mark.asyncio
async def test_effects_execution_timing():
    """Test effects execution timing relative to state changes"""
    instance = EffectInstance()

    def state1_exit(ctx: hsm.Context, inst: EffectInstance, event: hsm.Event) -> None:
        inst.log_action('state1-exit')

    def transition_effect(ctx: hsm.Context, inst: EffectInstance, event: hsm.Event) -> None:
        inst.log_action('transition-effect')

    def state2_entry(ctx: hsm.Context, inst: EffectInstance, event: hsm.Event) -> None:
        inst.log_action('state2-entry')

    model = hsm.define('EffectTimingMachine',
        hsm.initial(hsm.target('state1')),
        hsm.state('state1',
            hsm.exit(state1_exit),
            hsm.transition(
                hsm.on('move'),
                hsm.target('../state2'),
                hsm.effect(transition_effect)
            )
        ),
        hsm.state('state2',
            hsm.entry(state2_entry)
        )
    )

    ctx = hsm.hsm.context.new_context()
    await hsm.started(ctx, instance, model)
    instance.log.clear()

    await instance.dispatch(ctx, hsm.Event(name='move'))

    # Verify order: exit -> effect -> entry
    expected_order = ['state1-exit', 'transition-effect', 'state2-entry']
    assert instance.log == expected_order

    await instance.stop(ctx)
