"""
Test error event handling
Tests error event generation and handling in various scenarios
"""

import asyncio
import pytest
from hsm import hsm
from hsm.hsm import Instance, Event


class ErrorInstance(Instance):
    def __init__(self):
        super().__init__()
        self.log = []
        self.data = {}

    def log_action(self, action):
        self.log.append(action)


@pytest.mark.asyncio
async def test_error_event_from_activity_exception():
    """Error event from activity exception"""
    instance = ErrorInstance()

    async def failing_activity(ctx, inst, event):
        inst.log_action('activity-starting')
        raise Exception('Activity failed!')

    async def error_effect(ctx, inst, event):
        inst.log_action('error-caught')
        inst.data['error_message'] = str(event.data)
        inst.data['error_type'] = type(event.data).__name__

    async def error_entry(ctx, inst, event):
        inst.log_action('error-state-entry')

    model = hsm.define('ActivityErrorMachine',
        hsm.initial(hsm.target('working')),
        hsm.state('working',
            hsm.activity(failing_activity),
            hsm.transition(
                hsm.on('hsm_error'),
                hsm.target('../error'),
                hsm.effect(error_effect)
            )
        ),
        hsm.state('error',
            hsm.entry(error_entry)
        )
    )

    ctx = hsm.Context()
    sm = await hsm.start(ctx, instance, model)

    # Wait for error event to be dispatched
    await asyncio.sleep(0.02)  # 20ms

    assert 'activity-starting' in instance.log
    assert 'error-caught' in instance.log
    assert 'error-state-entry' in instance.log
    assert sm.state() == '/ActivityErrorMachine/error'
    assert 'Activity failed!' in instance.data['error_message']
    assert isinstance(instance.data['error_type'], str)

    await hsm.stop(sm)


@pytest.mark.asyncio
async def test_error_event_handled_at_different_hierarchy_levels():
    """Error event handled at different hierarchy levels"""
    instance = ErrorInstance()

    async def child_activity_error(ctx, inst, event):
        inst.log_action('child-activity-error')
        raise Exception('Child error')

    async def parent_error_effect(ctx, inst, event):
        inst.log_action('parent-handled-error')

    async def child_error_effect(ctx, inst, event):
        inst.log_action('child-handled-error')

    async def child_error_entry(ctx, inst, event):
        inst.log_action('child-error-entry')

    async def parent_error_entry(ctx, inst, event):
        inst.log_action('parent-error-entry')

    model = hsm.define('HierarchicalErrorMachine',
        hsm.initial(hsm.target('parent/child')),
        hsm.state('parent',
            hsm.transition(
                hsm.on('hsm_error'),
                hsm.target('parentError'),
                hsm.effect(parent_error_effect)
            ),
            hsm.state('child',
                hsm.activity(child_activity_error),
                hsm.transition(
                    hsm.on('hsm_error'),
                    hsm.target('../childError'),
                    hsm.effect(child_error_effect)
                )
            ),
            hsm.state('childError',
                hsm.entry(child_error_entry)
            ),
            hsm.state('parentError',
                hsm.entry(parent_error_entry)
            )
        )
    )

    ctx = hsm.Context()
    sm = await hsm.start(ctx, instance, model)

    # Wait for error event to bubble up
    await asyncio.sleep(0.02)  # 20ms

    # Child should handle its own error
    assert 'child-activity-error' in instance.log
    assert 'child-handled-error' in instance.log
    assert 'child-error-entry' in instance.log
    assert sm.state() == '/HierarchicalErrorMachine/parent/childError'

    # Parent error handler should not be called
    assert 'parent-handled-error' not in instance.log

    await hsm.stop(sm)


@pytest.mark.asyncio
async def test_unhandled_error_events():
    """Unhandled error events"""
    instance = ErrorInstance()

    async def failing_activity(ctx, inst, event):
        inst.log_action('activity-will-fail')
        raise Exception('Unhandled error!')

    model = hsm.define('UnhandledErrorMachine',
        hsm.initial(hsm.target('fragile')),
        hsm.state('fragile',
            hsm.activity(failing_activity)
            # No error transition handler
        )
    )

    ctx = hsm.Context()
    sm = await hsm.start(ctx, instance, model)

    # Wait for error - should remain in same state since unhandled
    await asyncio.sleep(0.02)  # 20ms

    assert 'activity-will-fail' in instance.log
    # Should remain in fragile state since error is unhandled
    assert sm.state() == '/UnhandledErrorMachine/fragile'

    await hsm.stop(sm)


@pytest.mark.asyncio
async def test_error_in_entry_actions():
    """Error in entry actions"""
    instance = ErrorInstance()

    async def failing_entry(ctx, inst, event):
        inst.log_action('entry-will-fail')
        raise Exception('Entry action failed!')

    async def error_effect(ctx, inst, event):
        inst.log_action('caught-entry-error')
        inst.data['entry_error'] = str(event.data)

    model = hsm.define('EntryErrorMachine',
        hsm.initial(hsm.target('start')),
        hsm.state('start',
            hsm.transition(
                hsm.on('go'),
                hsm.target('../failing')
            )
        ),
        hsm.state('failing',
            hsm.entry(failing_entry)
        ),
        hsm.state('error'),
        hsm.transition(
            hsm.on('hsm_error'),
            hsm.target('/error'),
            hsm.effect(error_effect)
        )
    )

    ctx = hsm.Context()
    sm = await hsm.start(ctx, instance, model)

    # Trigger transition to failing state
    await sm.dispatch(Event('go'))
    await asyncio.sleep(0.02)  # 20ms

    assert 'entry-will-fail' in instance.log
    assert 'caught-entry-error' in instance.log
    assert 'Entry action failed!' in instance.data['entry_error']

    await hsm.stop(sm)


@pytest.mark.asyncio
async def test_error_in_exit_actions():
    """Error in exit actions"""
    instance = ErrorInstance()

    async def failing_exit(ctx, inst, event):
        inst.log_action('exit-will-fail')
        raise Exception('Exit action failed!')

    async def error_effect(ctx, inst, event):
        inst.log_action('caught-exit-error')
        inst.data['exit_error'] = str(event.data)

    model = hsm.define('ExitErrorMachine',
        hsm.initial(hsm.target('unstable')),
        hsm.state('unstable',
            hsm.exit(failing_exit),
            hsm.transition(
                hsm.on('leave'),
                hsm.target('../next')
            )
        ),
        hsm.state('next'),
        hsm.state('error'),
        hsm.transition(
            hsm.on('hsm_error'),
            hsm.target('/error'),
            hsm.effect(error_effect)
        )
    )

    ctx = hsm.Context()
    sm = await hsm.start(ctx, instance, model)

    # Trigger transition that will cause exit action to fail
    await sm.dispatch(Event('leave'))
    await asyncio.sleep(0.02)  # 20ms

    assert 'exit-will-fail' in instance.log
    assert 'caught-exit-error' in instance.log
    assert 'Exit action failed!' in instance.data['exit_error']

    await hsm.stop(sm)


@pytest.mark.asyncio
async def test_error_in_transition_effects():
    """Error in transition effects"""
    instance = ErrorInstance()

    async def failing_effect(ctx, inst, event):
        inst.log_action('effect-will-fail')
        raise Exception('Effect failed!')

    async def error_effect(ctx, inst, event):
        inst.log_action('caught-effect-error')
        inst.data['effect_error'] = str(event.data)

    model = hsm.define('EffectErrorMachine',
        hsm.initial(hsm.target('start')),
        hsm.state('start',
            hsm.transition(
                hsm.on('trigger'),
                hsm.target('../next'),
                hsm.effect(failing_effect)
            )
        ),
        hsm.state('next'),
        hsm.state('error'),
        hsm.transition(
            hsm.on('hsm_error'),
            hsm.target('/error'),
            hsm.effect(error_effect)
        )
    )

    ctx = hsm.Context()
    sm = await hsm.start(ctx, instance, model)

    # Trigger transition with failing effect
    await sm.dispatch(Event('trigger'))
    await asyncio.sleep(0.02)  # 20ms

    assert 'effect-will-fail' in instance.log
    assert 'caught-effect-error' in instance.log
    assert 'Effect failed!' in instance.data['effect_error']

    await hsm.stop(sm)


@pytest.mark.asyncio
async def test_multiple_error_events_in_sequence():
    """Multiple error events in sequence"""
    instance = ErrorInstance()

    async def first_error_activity(ctx, inst, event):
        inst.log_action('first-error')
        raise Exception('First error!')

    async def second_error_activity(ctx, inst, event):
        inst.log_action('second-error')
        raise Exception('Second error!')

    async def first_error_effect(ctx, inst, event):
        inst.log_action('handled-first-error')

    async def second_error_effect(ctx, inst, event):
        inst.log_action('handled-second-error')

    model = hsm.define('MultipleErrorMachine',
        hsm.initial(hsm.target('first')),
        hsm.state('first',
            hsm.activity(first_error_activity),
            hsm.transition(
                hsm.on('hsm_error'),
                hsm.target('../second'),
                hsm.effect(first_error_effect)
            )
        ),
        hsm.state('second',
            hsm.activity(second_error_activity),
            hsm.transition(
                hsm.on('hsm_error'),
                hsm.target('../final'),
                hsm.effect(second_error_effect)
            )
        ),
        hsm.state('final')
    )

    ctx = hsm.Context()
    sm = await hsm.start(ctx, instance, model)

    # Wait for both errors to be handled
    await asyncio.sleep(0.05)  # 50ms

    assert 'first-error' in instance.log
    assert 'handled-first-error' in instance.log
    assert 'second-error' in instance.log
    assert 'handled-second-error' in instance.log
    assert sm.state() == '/MultipleErrorMachine/final'

    await hsm.stop(sm)


@pytest.mark.asyncio
async def test_error_event_with_guard_conditions():
    """Error event handling with guard conditions for error routing"""
    instance = ErrorInstance()
    instance.data['error_type'] = 'critical'

    async def error_activity(ctx, inst, event):
        inst.log_action('error-activity')
        raise Exception('Routed error!')

    async def critical_guard(ctx, inst, event):
        return inst.data['error_type'] == 'critical'

    async def warning_guard(ctx, inst, event):
        return inst.data['error_type'] == 'warning'

    async def critical_effect(ctx, inst, event):
        inst.log_action('critical-error-handled')

    async def warning_effect(ctx, inst, event):
        inst.log_action('warning-error-handled')

    async def general_effect(ctx, inst, event):
        inst.log_action('general-error-handled')

    model = hsm.define('GuardedErrorMachine',
        hsm.initial(hsm.target('working')),
        hsm.state('working',
            hsm.activity(error_activity),
            hsm.transition(
                hsm.on('hsm_error'),
                hsm.guard(critical_guard),
                hsm.target('../critical'),
                hsm.effect(critical_effect)
            ),
            hsm.transition(
                hsm.on('hsm_error'),
                hsm.guard(warning_guard),
                hsm.target('../warning'),
                hsm.effect(warning_effect)
            ),
            hsm.transition(
                hsm.on('hsm_error'),
                hsm.target('../general'),
                hsm.effect(general_effect)
            )
        ),
        hsm.state('critical'),
        hsm.state('warning'),
        hsm.state('general')
    )

    ctx = hsm.Context()
    sm = await hsm.start(ctx, instance, model)

    # Wait for error to be routed to critical handler
    await asyncio.sleep(0.02)  # 20ms

    assert 'error-activity' in instance.log
    assert 'critical-error-handled' in instance.log
    assert 'warning-error-handled' not in instance.log
    assert 'general-error-handled' not in instance.log
    assert sm.state() == '/GuardedErrorMachine/critical'

    await hsm.stop(sm)