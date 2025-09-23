"""
Test timer-based transitions (after/every)
Tests time-based events that fire after delays or at intervals
"""

import asyncio
import pytest
from datetime import timedelta
from hsm import hsm
from hsm.hsm import Instance, Event


class TimerInstance(Instance):
    def __init__(self):
        super().__init__()
        self.log = []
        self.data = {
            'tick_count': 0,
            'count': 0,
            'delay': 0,
            'timer_event': None
        }

    def log_action(self, action):
        self.log.append(action)


@pytest.mark.asyncio
async def test_basic_after_timer_fires_once_after_delay():
    """Basic after timer - fires once after delay"""
    instance = TimerInstance()

    async def waiting_entry(ctx, inst, event):
        inst.log_action('waiting-entry')

    async def timer_triggered_effect(ctx, inst, event):
        inst.log_action('timer-triggered')

    async def done_entry(ctx, inst, event):
        inst.log_action('done-entry')

    async def after_delay(ctx, inst, event):
        return timedelta(milliseconds=50)  # 50ms delay

    model = hsm.define('BasicAfterMachine',
        hsm.initial(hsm.target('waiting')),
        hsm.state('waiting',
            hsm.entry(waiting_entry),
            hsm.transition(
                hsm.after(after_delay),
                hsm.target('/done'),
                hsm.effect(timer_triggered_effect)
            )
        ),
        hsm.state('done',
            hsm.entry(done_entry)
        )
    )

    ctx = hsm.Context()
    sm = await hsm.start(ctx, instance, model)
    assert instance.log == ['waiting-entry']
    assert sm.state() == '/BasicAfterMachine/waiting'

    # Wait for timer to fire
    await asyncio.sleep(0.1)  # 100ms

    assert instance.log == [
        'waiting-entry',
        'timer-triggered',
        'done-entry'
    ]
    assert sm.state() == '/BasicAfterMachine/done'

    await hsm.stop(sm)


@pytest.mark.asyncio
async def test_after_timer_aborted_on_state_exit():
    """After timer aborted on state exit"""
    instance = TimerInstance()

    async def timeout_effect(ctx, inst, event):
        inst.log_action('timeout-fired')

    async def cancel_effect(ctx, inst, event):
        inst.log_action('manual-cancel')

    async def long_delay(ctx, inst, event):
        return timedelta(milliseconds=100)  # Long delay

    model = hsm.define('AbortedAfterMachine',
        hsm.initial(hsm.target('timed')),
        hsm.state('timed',
            hsm.transition(
                hsm.after(long_delay),
                hsm.target('/timeout'),
                hsm.effect(timeout_effect)
            ),
            hsm.transition(
                hsm.on('cancel'),
                hsm.target('/cancelled'),
                hsm.effect(cancel_effect)
            )
        ),
        hsm.state('timeout'),
        hsm.state('cancelled')
    )

    ctx = hsm.Context()
    sm = await hsm.start(ctx, instance, model)

    # Cancel before timer fires
    await asyncio.sleep(0.03)  # 30ms
    await sm.dispatch(Event(name='cancel'))

    assert instance.log == ['manual-cancel']
    assert sm.state() == '/AbortedAfterMachine/cancelled'

    # Wait longer to ensure timer doesn't fire
    await asyncio.sleep(0.1)  # 100ms
    assert 'timeout-fired' not in instance.log

    await hsm.stop(sm)


@pytest.mark.asyncio
async def test_basic_every_timer_fires_repeatedly_at_intervals():
    """Basic every timer - fires repeatedly at intervals"""
    instance = TimerInstance()

    async def counting_entry(ctx, inst, event):
        inst.data['count'] = 0
        inst.log_action('counting-entry')

    async def tick_effect(ctx, inst, event):
        inst.data['count'] += 1
        inst.log_action(f'tick-{inst.data["count"]}')

    async def stop_effect(ctx, inst, event):
        inst.log_action(f'stopped-at-{inst.data["count"]}')

    async def every_interval(ctx, inst, event):
        return timedelta(milliseconds=30)  # 30ms interval

    model = hsm.define('BasicEveryMachine',
        hsm.initial(hsm.target('counting')),
        hsm.state('counting',
            hsm.entry(counting_entry),
            hsm.transition(
                hsm.every(every_interval),
                hsm.effect(tick_effect)
            ),
            hsm.transition(
                hsm.on('stop'),
                hsm.target('/stopped'),
                hsm.effect(stop_effect)
            )
        ),
        hsm.state('stopped')
    )

    ctx = hsm.Context()
    sm = await hsm.start(ctx, instance, model)
    assert instance.log == ['counting-entry']

    # Let it tick a few times
    await asyncio.sleep(0.1)  # 100ms

    # Should have ticked at least 2-3 times
    assert instance.data['count'] >= 2
    assert 'tick-1' in instance.log
    assert 'tick-2' in instance.log

    # Stop the timer
    await sm.dispatch(Event(name='stop'))

    final_count = instance.data['count']
    assert f'stopped-at-{final_count}' in instance.log

    # Wait and ensure no more ticks
    await asyncio.sleep(0.05)  # 50ms
    assert instance.data['count'] == final_count

    await hsm.stop(sm)


@pytest.mark.asyncio
async def test_multiple_timers_in_same_state():
    """Multiple timers in same state"""
    instance = TimerInstance()

    async def timer1_effect(ctx, inst, event):
        inst.log_action('timer1-fired')

    async def timer2_effect(ctx, inst, event):
        inst.log_action('timer2-fired')

    async def delay1(ctx, inst, event):
        return timedelta(milliseconds=40)  # 40ms

    async def delay2(ctx, inst, event):
        return timedelta(milliseconds=80)  # 80ms

    model = hsm.define('MultipleTimerMachine',
        hsm.initial(hsm.target('multi')),
        hsm.state('multi',
            hsm.transition(
                hsm.after(delay1),
                hsm.target('/path1'),
                hsm.effect(timer1_effect)
            ),
            hsm.transition(
                hsm.after(delay2),
                hsm.target('/path2'),
                hsm.effect(timer2_effect)
            )
        ),
        hsm.state('path1'),
        hsm.state('path2')
    )

    ctx = hsm.Context()
    sm = await hsm.start(ctx, instance, model)

    # First timer should fire first
    await asyncio.sleep(0.06)  # 60ms
    assert instance.log == ['timer1-fired']
    assert sm.state() == '/MultipleTimerMachine/path1'

    # Second timer should not fire because we've exited the state
    await asyncio.sleep(0.04)  # 40ms
    assert 'timer2-fired' not in instance.log

    await hsm.stop(sm)


@pytest.mark.asyncio
async def test_timer_with_dynamic_duration_based_on_instance_data():
    """Timer with dynamic duration based on instance data"""
    instance = TimerInstance()
    instance.data['delay'] = 60

    async def waiting_entry(ctx, inst, event):
        inst.log_action(f'waiting-with-delay-{inst.data["delay"]}')

    async def dynamic_timer_effect(ctx, inst, event):
        inst.log_action('dynamic-timer-fired')

    async def dynamic_delay(ctx, inst, event):
        return timedelta(milliseconds=inst.data['delay'])

    model = hsm.define('DynamicTimerMachine',
        hsm.initial(hsm.target('waiting')),
        hsm.state('waiting',
            hsm.entry(waiting_entry),
            hsm.transition(
                hsm.after(dynamic_delay),
                hsm.target('/finished'),
                hsm.effect(dynamic_timer_effect)
            )
        ),
        hsm.state('finished')
    )

    ctx = hsm.Context()
    sm = await hsm.start(ctx, instance, model)
    assert instance.log == ['waiting-with-delay-60']

    # Timer should fire after instance.data.delay ms
    await asyncio.sleep(0.08)  # 80ms
    assert instance.log == [
        'waiting-with-delay-60',
        'dynamic-timer-fired'
    ]
    assert sm.state() == '/DynamicTimerMachine/finished'

    await hsm.stop(sm)


@pytest.mark.asyncio
async def test_timer_with_event_data_access():
    """Timer with event data access"""
    instance = TimerInstance()

    async def event_data_timer(ctx, inst, event):
        inst.data['timer_event'] = event
        return timedelta(milliseconds=50)

    model = hsm.define('EventDataTimerMachine',
        hsm.initial(hsm.target('timed')),
        hsm.state('timed',
            hsm.transition(
                hsm.after(event_data_timer),
                hsm.target('/triggered')
            )
        ),
        hsm.state('triggered')
    )

    ctx = hsm.Context()
    sm = await hsm.start(ctx, instance, model)

    # Wait a brief moment for the timer activity to start and call the duration function
    await asyncio.sleep(0.01)  # 10ms

    # Timer function should receive initial event
    assert instance.data['timer_event'] is not None
    assert instance.data['timer_event'].name == 'hsm_initial'

    await asyncio.sleep(0.07)  # 70ms
    assert sm.state() == '/EventDataTimerMachine/triggered'

    await hsm.stop(sm)


@pytest.mark.asyncio
async def test_zero_or_negative_timer_duration():
    """Zero or negative timer duration"""
    instance = TimerInstance()

    async def immediate_effect(ctx, inst, event):
        inst.log_action('immediate-timer')

    async def zero_delay(ctx, inst, event):
        return timedelta(milliseconds=0)  # Immediate

    model = hsm.define('ZeroTimerMachine',
        hsm.initial(hsm.target('immediate')),
        hsm.state('immediate',
            hsm.transition(
                hsm.after(zero_delay),
                hsm.target('/done'),
                hsm.effect(immediate_effect)
            )
        ),
        hsm.state('done')
    )

    ctx = hsm.Context()
    sm = await hsm.start(ctx, instance, model)

    # Should not create timer for zero duration
    await asyncio.sleep(0.02)  # 20ms

    # State should remain unchanged
    assert sm.state() == '/ZeroTimerMachine/immediate'
    assert 'immediate-timer' not in instance.log

    await hsm.stop(sm)


@pytest.mark.asyncio
async def test_timer_in_hierarchical_state():
    """Timer in hierarchical state"""
    instance = TimerInstance()

    async def parent_timeout_effect(ctx, inst, event):
        inst.log_action('parent-handled-timeout')

    async def hier_delay(ctx, inst, event):
        return timedelta(milliseconds=50)

    model = hsm.define('HierarchicalTimerMachine',
        hsm.initial(hsm.target('parent/child')),
        hsm.state('parent',
            hsm.state('child',
                hsm.transition(
                    hsm.after(hier_delay),
                    hsm.target('/done'),
                    hsm.effect(parent_timeout_effect)
                )
            )
        ),
        hsm.state('done')
    )

    ctx = hsm.Context()
    sm = await hsm.start(ctx, instance, model)
    assert sm.state() == '/HierarchicalTimerMachine/parent/child'

    # Timer should fire and bubble up
    await asyncio.sleep(0.07)  # 70ms

    assert instance.log == ['parent-handled-timeout']
    assert sm.state() == '/HierarchicalTimerMachine/done'

    await hsm.stop(sm)


@pytest.mark.asyncio
async def test_every_timer_with_abort_signal_handling():
    """Every timer with abort signal handling"""
    instance = TimerInstance()

    async def active_entry(ctx, inst, event):
        # Only reset tick_count on initial entry, not on self-transitions
        if 'tick_count' not in inst.data:
            inst.data['tick_count'] = 0

    async def every_tick_effect(ctx, inst, event):
        inst.data['tick_count'] += 1
        inst.log_action(f'tick-{inst.data["tick_count"]}')

    async def finished_entry(ctx, inst, event):
        inst.log_action(f'finished-at-tick-{inst.data["tick_count"]}')

    async def tick_interval(ctx, inst, event):
        return timedelta(milliseconds=25)  # 25ms

    model = hsm.define('EveryTimerAbortMachine',
        hsm.initial(hsm.target('active')),
        hsm.state('active',
            hsm.entry(active_entry),
            hsm.transition(
                hsm.every(tick_interval),
                hsm.target('.'),  # Self transition
                hsm.effect(every_tick_effect)
            ),
            hsm.transition(
                hsm.on('finish'),
                hsm.target('../finished')
            )
        ),
        hsm.state('finished',
            hsm.entry(finished_entry)
        )
    )

    ctx = hsm.Context()
    sm = await hsm.start(ctx, instance, model)

    # Let it tick a few times
    await asyncio.sleep(0.08)  # 80ms
    assert instance.data['tick_count'] >= 2

    # Stop while ticking
    await sm.dispatch(Event(name='finish'))
    final_tick = instance.data['tick_count']

    # Wait and ensure no more ticks
    await asyncio.sleep(0.05)  # 50ms
    assert instance.data['tick_count'] == final_tick
    assert f'finished-at-tick-{final_tick}' in instance.log

    await hsm.stop(sm)