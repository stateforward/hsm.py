"""
Test basic state transitions and state machine lifecycle
Tests fundamental HSM features including state transitions, lifecycle methods, and basic event handling
"""

import pytest
import asyncio
import hsm


class BasicInstance(hsm.Instance):
    """Test instance implementation for basic state testing"""
    
    def __init__(self):
        super().__init__()
        self.log = []
        self.data = {}

    def log_action(self, action: str):
        """Log an action for test verification"""
        self.log.append(action)


@pytest.mark.asyncio
async def test_basic_state_machine_with_simple_transitions():
    """Test basic state machine with simple transitions"""
    instance = BasicInstance()

    model = hsm.define('BasicMachine',
        hsm.initial(hsm.target('idle')),
        hsm.state('idle',
            hsm.transition(
                hsm.on('start'),
                hsm.target('../running')
            )
        ),
        hsm.state('running',
            hsm.transition(
                hsm.on('stop'),
                hsm.target('../idle')
            )
        )
    )

    # Start the state machine
    ctx = hsm.Context()
    await hsm.start(ctx, instance, model)

    # Should start in idle state
    assert instance.state() == '/BasicMachine/idle'

    # Dispatch start event
    await instance.dispatch(hsm.Event(name='start'))
    assert instance.state() == '/BasicMachine/running'

    # Dispatch stop event
    await instance.dispatch(hsm.Event(name='stop'))
    assert instance.state() == '/BasicMachine/idle'

    # Stop the state machine
    await hsm.stop(instance)


@pytest.mark.asyncio
async def test_state_machine_lifecycle_start_and_stop():
    """Test state machine lifecycle - start and stop"""
    instance = BasicInstance()

    async def active_entry(ctx: hsm.Context, inst: BasicInstance, event: hsm.Event) -> None:
        inst.log_action('active-entry')

    async def active_exit(ctx: hsm.Context, inst: BasicInstance, event: hsm.Event) -> None:
        inst.log_action('active-exit')

    model = hsm.define('LifecycleMachine',
        hsm.initial(hsm.target('active')),
        hsm.state('active',
            hsm.entry(active_entry),
            hsm.exit(active_exit)
        )
    )

    # Start the state machine
    ctx = hsm.Context()
    await hsm.start(ctx, instance, model)

    # Should have executed entry action
    assert 'active-entry' in instance.log
    assert instance.state() == '/LifecycleMachine/active'

    # Stop the state machine
    await hsm.stop(instance)

    # Should have executed exit action
    assert 'active-exit' in instance.log

    # State should be reset to model (root)
    assert instance.state() == '/LifecycleMachine'


@pytest.mark.asyncio
async def test_multiple_transitions_from_same_state():
    """Test multiple transitions from the same state"""
    instance = BasicInstance()

    model = hsm.define('MultiTransitionMachine',
        hsm.initial(hsm.target('idle')),
        hsm.state('idle',
            hsm.transition(
                hsm.on('event1'),
                hsm.target('../state1')
            ),
            hsm.transition(
                hsm.on('event2'),
                hsm.target('../state2')
            )
        ),
        hsm.state('state1',
            hsm.transition(
                hsm.on('back'),
                hsm.target('../idle')
            )
        ),
        hsm.state('state2',
            hsm.transition(
                hsm.on('back'),
                hsm.target('../idle')
            )
        )
    )

    ctx = hsm.Context()
    await hsm.start(ctx, instance, model)

    # Test first transition
    await instance.dispatch(hsm.Event(name='event1'))
    assert instance.state() == '/MultiTransitionMachine/state1'

    # Go back to idle
    await instance.dispatch(hsm.Event(name='back'))
    assert instance.state() == '/MultiTransitionMachine/idle'

    # Test second transition
    await instance.dispatch(hsm.Event(name='event2'))
    assert instance.state() == '/MultiTransitionMachine/state2'

    await hsm.stop(instance)


@pytest.mark.asyncio
async def test_self_transitions():
    """Test self transitions"""
    instance = BasicInstance()

    async def counter_entry(ctx: hsm.Context, inst: BasicInstance, event: hsm.Event) -> None:
        inst.data['count'] = inst.data.get('count', 0) + 1
        inst.log_action(f'counter-entry-{inst.data["count"]}')

    async def counter_exit(ctx: hsm.Context, inst: BasicInstance, event: hsm.Event) -> None:
        inst.log_action(f'counter-exit-{inst.data["count"]}')

    async def increment_effect(ctx: hsm.Context, inst: BasicInstance, event: hsm.Event) -> None:
        inst.log_action('increment-effect')

    model = hsm.define('SelfTransitionMachine',
        hsm.initial(hsm.target('counter')),
        hsm.state('counter',
            hsm.entry(counter_entry),
            hsm.exit(counter_exit),
            hsm.transition(
                hsm.on('increment'),
                hsm.target('../counter'),  # Self transition
                hsm.effect(increment_effect)
            )
        )
    )

    ctx = hsm.Context()
    await hsm.start(ctx, instance, model)

    # Initial entry
    assert instance.data['count'] == 1
    assert 'counter-entry-1' in instance.log

    # Self transition should exit and re-enter the state
    await instance.dispatch(hsm.Event(name='increment'))
    assert instance.data['count'] == 2
    expected_sequence = ['counter-entry-1', 'counter-exit-1', 'increment-effect', 'counter-entry-2']
    
    # Check that all expected actions occurred
    for action in expected_sequence:
        assert action in instance.log

    await hsm.stop(instance)


@pytest.mark.asyncio
async def test_internal_transitions():
    """Test internal transitions"""
    instance = BasicInstance()

    async def active_entry(ctx: hsm.Context, inst: BasicInstance, event: hsm.Event) -> None:
        inst.data['entry_count'] = inst.data.get('entry_count', 0) + 1
        inst.log_action('active-entry')

    async def active_exit(ctx: hsm.Context, inst: BasicInstance, event: hsm.Event) -> None:
        inst.log_action('active-exit')

    async def internal_effect(ctx: hsm.Context, inst: BasicInstance, event: hsm.Event) -> None:
        inst.log_action('internal-effect')

    model = hsm.define('InternalTransitionMachine',
        hsm.initial(hsm.target('active')),
        hsm.state('active',
            hsm.entry(active_entry),
            hsm.exit(active_exit),
            hsm.transition(
                hsm.on('internal'),
                # No target specified makes it internal
                hsm.effect(internal_effect)
            )
        )
    )

    ctx = hsm.Context()
    await hsm.start(ctx, instance, model)

    # Initial entry
    assert instance.data['entry_count'] == 1
    assert 'active-entry' in instance.log

    # Internal transition should NOT exit/enter the state
    await instance.dispatch(hsm.Event(name='internal'))
    assert instance.data['entry_count'] == 1  # Should not re-enter
    assert 'internal-effect' in instance.log
    assert 'active-exit' not in instance.log  # Should not exit

    await hsm.stop(instance)


@pytest.mark.asyncio
async def test_event_dispatching_during_lifecycle():
    """Test event dispatching during state machine lifecycle"""
    instance = BasicInstance()

    model = hsm.define('EventLifecycleMachine',
        hsm.initial(hsm.target('a')),
        hsm.state('a',
            hsm.transition(
                hsm.on('next'),
                hsm.target('../b')
            )
        ),
        hsm.state('b')
    )

    # Start the state machine
    ctx = hsm.Context()
    sm = await hsm.start(ctx, instance, model)
    assert instance.state() == '/EventLifecycleMachine/a'

    # Now event should work
    await sm.dispatch(hsm.Event(name='next'))
    assert instance.state() == '/EventLifecycleMachine/b'

    # Stop the state machine
    await hsm.stop(sm)
    assert instance.state() == '/EventLifecycleMachine'  # Root state after stop


@pytest.mark.asyncio
async def test_unknown_events_ignored():
    """Test unknown events should be ignored"""
    instance = BasicInstance()

    async def stable_entry(ctx: hsm.Context, inst: BasicInstance, event: hsm.Event) -> None:
        inst.log_action('stable-entry')

    model = hsm.define('UnknownEventMachine',
        hsm.initial(hsm.target('stable')),
        hsm.state('stable',
            hsm.entry(stable_entry)
        )
    )

    ctx = hsm.Context()
    await hsm.start(ctx, instance, model)
    assert instance.state() == '/UnknownEventMachine/stable'

    # Dispatch unknown events
    await instance.dispatch(hsm.Event(name='unknown1'))
    await instance.dispatch(hsm.Event(name='unknown2'))
    await instance.dispatch(hsm.Event(name='unknown3', kind=hsm.Kinds.Event))

    # State should remain unchanged
    assert instance.state() == '/UnknownEventMachine/stable'
    # No additional actions should have been triggered
    assert instance.log == ['stable-entry']

    await hsm.stop(instance)


@pytest.mark.asyncio
async def test_event_object_dispatching():
    """Test event object dispatching"""
    instance = BasicInstance()

    async def proceed_effect(ctx: hsm.Context, inst: BasicInstance, event: hsm.Event) -> None:
        inst.log_action(f'effect-{event.name}')
        # Store event information in instance data for testing
        inst.data['last_event_name'] = event.name
        inst.data['last_event_kind'] = event.kind

    model = hsm.define('EventTypeMachine',
        hsm.initial(hsm.target('waiting')),
        hsm.state('waiting',
            hsm.transition(
                hsm.on('proceed'),
                hsm.target('../done'),
                hsm.effect(proceed_effect)
            )
        ),
        hsm.state('done')
    )

    ctx = hsm.Context()
    await hsm.start(ctx, instance, model)

    # Test Event object dispatching
    await instance.dispatch(hsm.Event(name='proceed'))
    assert instance.state() == '/EventTypeMachine/done'
    assert 'effect-proceed' in instance.log
    assert instance.data.get('last_event_name') == 'proceed'
    assert instance.data.get('last_event_kind') == hsm.Kinds.Event

    # Reset
    await hsm.stop(instance)
    instance.log = []
    instance.data = {}
    ctx = hsm.Context()
    await hsm.start(ctx, instance, model)

    # Test different event with explicit kind
    await instance.dispatch(hsm.Event(
        name='proceed',
        kind=hsm.Kinds.Event
    ))
    assert instance.state() == '/EventTypeMachine/done'
    assert 'effect-proceed' in instance.log
    assert instance.data.get('last_event_name') == 'proceed'
    assert instance.data.get('last_event_kind') == hsm.Kinds.Event

    await hsm.stop(instance)