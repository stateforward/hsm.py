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


class ManualClock:
    def __init__(self):
        self.sleeps: list[tuple[timedelta, asyncio.Future[None]]] = []
        self.cancelled = 0

    async def sleep(self, duration: timedelta) -> None:
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self.sleeps.append((duration, future))
        try:
            await future
        except asyncio.CancelledError:
            self.cancelled += 1
            raise

    def After(self, duration: timedelta) -> asyncio.Task[None]:
        return asyncio.create_task(self.sleep(duration))

    def _prune_done(self) -> None:
        self.sleeps = [(duration, future) for duration, future in self.sleeps if not future.done()]

    async def wait_for_sleep(self, count: int = 1) -> None:
        for _ in range(100):
            self._prune_done()
            if len(self.sleeps) >= count:
                return
            await asyncio.sleep(0)
        raise AssertionError(f"expected {count} scheduled sleeps, got {len(self.sleeps)}")

    def release_next(self) -> timedelta:
        self._prune_done()
        duration, future = self.sleeps.pop(0)
        if not future.done():
            future.set_result(None)
        return duration

    def release_duration(self, expected: timedelta) -> None:
        self._prune_done()
        for index, (duration, future) in enumerate(self.sleeps):
            if duration == expected:
                self.sleeps.pop(index)
                if not future.done():
                    future.set_result(None)
                return
        raise AssertionError(f"expected scheduled sleep {expected}, got {[duration for duration, _ in self.sleeps]}")


async def wait_until(predicate, message: str) -> None:
    for _ in range(100):
        if predicate():
            return
        await asyncio.sleep(0)
    raise AssertionError(message)


@pytest.mark.asyncio
async def test_basic_after_timer_fires_once_after_delay():
    """Basic after timer - fires once after delay"""
    instance = TimerInstance()
    clock = ManualClock()

    async def waiting_entry(ctx, inst, event):
        inst.log_action('waiting-entry')

    def timer_triggered_effect(ctx, inst, event):
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
                hsm.target('../done'),
                hsm.effect(timer_triggered_effect)
            )
        ),
        hsm.state('done',
            hsm.entry(done_entry)
        )
    )

    ctx = hsm.Context()
    sm = await hsm.Started(ctx, instance, model, hsm.Config(Clock=clock))
    assert instance.log == ['waiting-entry']
    assert sm.state() == '/BasicAfterMachine/waiting'

    await clock.wait_for_sleep()
    assert clock.release_next() == timedelta(milliseconds=50)
    await asyncio.wait_for(hsm.AfterEntry(ctx, instance, "/BasicAfterMachine/done"), timeout=1)

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
    clock = ManualClock()

    def timeout_effect(ctx, inst, event):
        inst.log_action('timeout-fired')

    def cancel_effect(ctx, inst, event):
        inst.log_action('manual-cancel')

    async def long_delay(ctx, inst, event):
        return timedelta(milliseconds=100)  # Long delay

    model = hsm.define('AbortedAfterMachine',
        hsm.initial(hsm.target('timed')),
        hsm.state('timed',
            hsm.transition(
                hsm.after(long_delay),
                hsm.target('../timeout'),
                hsm.effect(timeout_effect)
            ),
            hsm.transition(
                hsm.on('cancel'),
                hsm.target('../cancelled'),
                hsm.effect(cancel_effect)
            )
        ),
        hsm.state('timeout'),
        hsm.state('cancelled')
    )

    ctx = hsm.Context()
    sm = await hsm.Started(ctx, instance, model, hsm.Config(Clock=clock))

    await clock.wait_for_sleep()
    await sm.dispatch(ctx, Event(name='cancel'))

    assert instance.log == ['manual-cancel']
    assert sm.state() == '/AbortedAfterMachine/cancelled'

    await wait_until(lambda: clock.cancelled >= 1, "timer sleep was not cancelled")
    clock._prune_done()
    await asyncio.sleep(0)
    assert 'timeout-fired' not in instance.log

    await hsm.stop(sm)


@pytest.mark.asyncio
async def test_basic_every_timer_fires_repeatedly_at_intervals():
    """Basic every timer - fires repeatedly at intervals"""
    instance = TimerInstance()
    clock = ManualClock()

    async def counting_entry(ctx, inst, event):
        inst.data['count'] = 0
        inst.log_action('counting-entry')

    def tick_effect(ctx, inst, event):
        inst.data['count'] += 1
        inst.log_action(f'tick-{inst.data["count"]}')

    def stop_effect(ctx, inst, event):
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
                hsm.target('../stopped'),
                hsm.effect(stop_effect)
            )
        ),
        hsm.state('stopped')
    )

    ctx = hsm.Context()
    sm = await hsm.Started(ctx, instance, model, hsm.Config(Clock=clock))
    assert instance.log == ['counting-entry']

    await clock.wait_for_sleep()
    assert clock.release_next() == timedelta(milliseconds=30)
    await wait_until(lambda: instance.data['count'] >= 1, "first tick did not fire")
    await clock.wait_for_sleep()
    assert clock.release_next() == timedelta(milliseconds=30)
    await wait_until(lambda: instance.data['count'] >= 2, "second tick did not fire")

    assert 'tick-1' in instance.log
    assert 'tick-2' in instance.log

    await sm.dispatch(ctx, Event(name='stop'))

    final_count = instance.data['count']
    assert f'stopped-at-{final_count}' in instance.log

    await wait_until(lambda: clock.cancelled >= 1, "repeating timer sleep was not cancelled")
    clock._prune_done()
    await asyncio.sleep(0)
    assert instance.data['count'] == final_count

    await hsm.stop(sm)


@pytest.mark.asyncio
async def test_multiple_timers_in_same_state():
    """Multiple timers in same state"""
    instance = TimerInstance()
    clock = ManualClock()

    def timer1_effect(ctx, inst, event):
        inst.log_action('timer1-fired')

    def timer2_effect(ctx, inst, event):
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
                hsm.target('../path1'),
                hsm.effect(timer1_effect)
            ),
            hsm.transition(
                hsm.after(delay2),
                hsm.target('../path2'),
                hsm.effect(timer2_effect)
            )
        ),
        hsm.state('path1'),
        hsm.state('path2')
    )

    ctx = hsm.Context()
    sm = await hsm.Started(ctx, instance, model, hsm.Config(Clock=clock))

    await clock.wait_for_sleep(2)
    assert sorted(duration for duration, _ in clock.sleeps) == [
        timedelta(milliseconds=40),
        timedelta(milliseconds=80),
    ]
    clock.release_duration(timedelta(milliseconds=40))
    await asyncio.wait_for(hsm.AfterEntry(ctx, instance, "/MultipleTimerMachine/path1"), timeout=1)
    assert instance.log == ['timer1-fired']
    assert sm.state() == '/MultipleTimerMachine/path1'

    await wait_until(lambda: clock.cancelled >= 1, "second timer sleep was not cancelled")
    clock._prune_done()
    await asyncio.sleep(0)
    assert 'timer2-fired' not in instance.log

    await hsm.stop(sm)


@pytest.mark.asyncio
async def test_timer_with_dynamic_duration_based_on_instance_data():
    """Timer with dynamic duration based on instance data"""
    instance = TimerInstance()
    instance.data['delay'] = 60
    clock = ManualClock()

    async def waiting_entry(ctx, inst, event):
        inst.log_action(f'waiting-with-delay-{inst.data["delay"]}')

    def dynamic_timer_effect(ctx, inst, event):
        inst.log_action('dynamic-timer-fired')

    async def dynamic_delay(ctx, inst, event):
        return timedelta(milliseconds=inst.data['delay'])

    model = hsm.define('DynamicTimerMachine',
        hsm.initial(hsm.target('waiting')),
        hsm.state('waiting',
            hsm.entry(waiting_entry),
            hsm.transition(
                hsm.after(dynamic_delay),
                hsm.target('../finished'),
                hsm.effect(dynamic_timer_effect)
            )
        ),
        hsm.state('finished')
    )

    ctx = hsm.Context()
    sm = await hsm.Started(ctx, instance, model, hsm.Config(Clock=clock))
    assert instance.log == ['waiting-with-delay-60']

    await clock.wait_for_sleep()
    assert clock.release_next() == timedelta(milliseconds=60)
    await asyncio.wait_for(hsm.AfterEntry(ctx, instance, "/DynamicTimerMachine/finished"), timeout=1)
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
    clock = ManualClock()

    async def event_data_timer(ctx, inst, event):
        inst.data['timer_event'] = event
        return timedelta(milliseconds=50)

    model = hsm.define('EventDataTimerMachine',
        hsm.initial(hsm.target('timed')),
        hsm.state('timed',
            hsm.transition(
                hsm.after(event_data_timer),
                hsm.target('../triggered')
            )
        ),
        hsm.state('triggered')
    )

    ctx = hsm.Context()
    sm = await hsm.Started(ctx, instance, model, hsm.Config(Clock=clock))

    await clock.wait_for_sleep()

    assert instance.data['timer_event'] is not None
    assert instance.data['timer_event'].name == 'hsm/initial'

    assert clock.release_next() == timedelta(milliseconds=50)
    await asyncio.wait_for(hsm.AfterEntry(ctx, instance, "/EventDataTimerMachine/triggered"), timeout=1)
    assert sm.state() == '/EventDataTimerMachine/triggered'

    await hsm.stop(sm)


@pytest.mark.asyncio
async def test_zero_or_negative_timer_duration():
    """Zero or negative timer duration"""
    instance = TimerInstance()
    clock = ManualClock()

    def immediate_effect(ctx, inst, event):
        inst.log_action('immediate-timer')

    async def zero_delay(ctx, inst, event):
        return timedelta(milliseconds=0)  # Immediate

    model = hsm.define('ZeroTimerMachine',
        hsm.initial(hsm.target('immediate')),
        hsm.state('immediate',
            hsm.transition(
                hsm.after(zero_delay),
                hsm.target('../done'),
                hsm.effect(immediate_effect)
            )
        ),
        hsm.state('done')
    )

    ctx = hsm.Context()
    sm = await hsm.Started(ctx, instance, model, hsm.Config(Clock=clock))

    await asyncio.sleep(0)

    assert sm.state() == '/ZeroTimerMachine/immediate'
    assert 'immediate-timer' not in instance.log
    assert clock.sleeps == []

    await hsm.stop(sm)


@pytest.mark.asyncio
async def test_timer_in_hierarchical_state():
    """Timer in hierarchical state"""
    instance = TimerInstance()
    clock = ManualClock()

    def parent_timeout_effect(ctx, inst, event):
        inst.log_action('parent-handled-timeout')

    async def hier_delay(ctx, inst, event):
        return timedelta(milliseconds=50)

    model = hsm.define('HierarchicalTimerMachine',
        hsm.initial(hsm.target('parent/child')),
        hsm.state('parent',
            hsm.state('child',
                hsm.transition(
                    hsm.after(hier_delay),
                    hsm.target('../../done'),
                    hsm.effect(parent_timeout_effect)
                )
            )
        ),
        hsm.state('done')
    )

    ctx = hsm.Context()
    sm = await hsm.Started(ctx, instance, model, hsm.Config(Clock=clock))
    assert sm.state() == '/HierarchicalTimerMachine/parent/child'

    await clock.wait_for_sleep()
    assert clock.release_next() == timedelta(milliseconds=50)
    await asyncio.wait_for(hsm.AfterEntry(ctx, instance, "/HierarchicalTimerMachine/done"), timeout=1)

    assert instance.log == ['parent-handled-timeout']
    assert sm.state() == '/HierarchicalTimerMachine/done'

    await hsm.stop(sm)


@pytest.mark.asyncio
async def test_every_timer_with_abort_signal_handling():
    """Every timer with abort signal handling"""
    instance = TimerInstance()
    clock = ManualClock()

    async def active_entry(ctx, inst, event):
        # Only reset tick_count on initial entry, not on self-transitions
        if 'tick_count' not in inst.data:
            inst.data['tick_count'] = 0

    def every_tick_effect(ctx, inst, event):
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
    sm = await hsm.Started(ctx, instance, model, hsm.Config(Clock=clock))

    await clock.wait_for_sleep()
    assert clock.release_next() == timedelta(milliseconds=25)
    await wait_until(lambda: instance.data['tick_count'] >= 1, "first self-transition tick did not fire")
    await clock.wait_for_sleep()
    assert clock.release_next() == timedelta(milliseconds=25)
    await wait_until(lambda: instance.data['tick_count'] >= 2, "second self-transition tick did not fire")

    cancelled_before_finish = clock.cancelled
    await sm.dispatch(ctx, Event(name='finish'))
    final_tick = instance.data['tick_count']

    await wait_until(
        lambda: clock.cancelled > cancelled_before_finish,
        "self-transition timer sleep was not cancelled on finish",
    )
    clock._prune_done()
    await asyncio.sleep(0)
    assert instance.data['tick_count'] == final_tick
    assert f'finished-at-tick-{final_tick}' in instance.log

    await hsm.stop(sm)
