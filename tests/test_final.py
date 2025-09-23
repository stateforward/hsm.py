"""
Test final states
Tests terminal states that indicate completion of a state machine region
"""

import pytest
from hsm import hsm
from hsm.hsm import Instance, Event
import time


class FinalInstance(Instance):
    def __init__(self):
        super().__init__()
        self.log = []
        self.data = {}

    def log_action(self, action):
        self.log.append(action)


@pytest.mark.asyncio
async def test_basic_final_state_terminal_state():
    """Basic final state - terminal state"""
    instance = FinalInstance()

    async def working_entry(ctx, inst, event):
        inst.log_action('working-entry')

    async def completing_effect(ctx, inst, event):
        inst.log_action('completing')

    model = hsm.define('BasicFinalMachine',
        hsm.initial(hsm.target('working')),
        hsm.state('working',
            hsm.entry(working_entry),
            hsm.transition(
                hsm.on('complete'),
                hsm.target('../done'),
                hsm.effect(completing_effect)
            )
        ),
        hsm.final('done')
    )

    ctx = hsm.Context()
    sm = await hsm.start(ctx, instance, model)
    assert instance.log == ['working-entry']
    assert sm.state() == '/BasicFinalMachine/working'

    # Transition to final state
    await sm.dispatch(Event('complete'))
    assert instance.log == ['working-entry', 'completing']
    assert sm.state() == '/BasicFinalMachine/done'

    # Final states should not accept events
    await sm.dispatch(Event('any-event'))
    assert sm.state() == '/BasicFinalMachine/done'

    await hsm.stop(sm)


@pytest.mark.asyncio
async def test_final_state_in_hierarchical_structure():
    """Final state in hierarchical structure"""
    instance = FinalInstance()

    async def container_entry(ctx, inst, event):
        inst.log_action('container-entry')

    async def subprocess_entry(ctx, inst, event):
        inst.log_action('subprocess-entry')

    async def step1_entry(ctx, inst, event):
        inst.log_action('step1-entry')

    async def step2_entry(ctx, inst, event):
        inst.log_action('step2-entry')

    async def aborted_entry(ctx, inst, event):
        inst.log_action('aborted-entry')

    model = hsm.define('HierarchicalFinalMachine',
        hsm.initial(hsm.target('container/subprocess')),
        hsm.state('container',
            hsm.entry(container_entry),
            hsm.state('subprocess',
                hsm.initial(hsm.target('step1')),
                hsm.entry(subprocess_entry),
                hsm.state('step1',
                    hsm.entry(step1_entry),
                    hsm.transition(
                        hsm.on('next'),
                        hsm.target('../step2')
                    )
                ),
                hsm.state('step2',
                    hsm.entry(step2_entry),
                    hsm.transition(
                        hsm.on('finish'),
                        hsm.target('../completed')
                    )
                ),
                hsm.final('completed')
            ),
            hsm.transition(
                hsm.on('abort'),
                hsm.target('../aborted')
            )
        ),
        hsm.state('aborted',
            hsm.entry(aborted_entry)
        )
    )

    ctx = hsm.Context()
    sm = await hsm.start(ctx, instance, model)
    assert instance.log == [
        'container-entry',
        'subprocess-entry',
        'step1-entry'
    ]
    assert sm.state() == '/HierarchicalFinalMachine/container/subprocess/step1'

    # Progress through subprocess
    await sm.dispatch(Event('next'))
    assert sm.state() == '/HierarchicalFinalMachine/container/subprocess/step2'

    # Complete subprocess
    await sm.dispatch(Event('finish'))
    assert sm.state() == '/HierarchicalFinalMachine/container/subprocess/completed'

    # Events can still be handled by parent
    await sm.dispatch(Event('abort'))
    assert sm.state() == '/HierarchicalFinalMachine/aborted'
    assert 'aborted-entry' in instance.log

    await hsm.stop(sm)


@pytest.mark.asyncio
async def test_multiple_final_states_in_same_container():
    """Multiple final states in same container"""
    instance = FinalInstance()

    async def running_entry(ctx, inst, event):
        inst.log_action('running-entry')

    model = hsm.define('MultipleFinalMachine',
        hsm.initial(hsm.target('process/running')),
        hsm.state('process',
            hsm.state('running',
                hsm.entry(running_entry),
                hsm.transition(
                    hsm.on('success'),
                    hsm.target('../success')
                ),
                hsm.transition(
                    hsm.on('error'),
                    hsm.target('../error')
                ),
                hsm.transition(
                    hsm.on('cancel'),
                    hsm.target('../cancelled')
                )
            ),
            hsm.final('success'),
            hsm.final('error'),
            hsm.final('cancelled')
        )
    )

    # Test success path
    ctx = hsm.Context()
    sm = await hsm.start(ctx, instance, model)
    await sm.dispatch(Event('success'))
    assert sm.state() == '/MultipleFinalMachine/process/success'
    await hsm.stop(sm)

    # Test error path
    instance.log = []
    ctx2 = hsm.Context()
    sm2 = await hsm.start(ctx2, instance, model)
    await sm2.dispatch(Event('error'))
    assert sm2.state() == '/MultipleFinalMachine/process/error'
    await hsm.stop(sm2)

    # Test cancel path
    instance.log = []
    ctx3 = hsm.Context()
    sm3 = await hsm.start(ctx3, instance, model)
    await sm3.dispatch(Event('cancel'))
    assert sm3.state() == '/MultipleFinalMachine/process/cancelled'
    await hsm.stop(sm3)


@pytest.mark.asyncio
async def test_transition_to_final_state_with_effect():
    """Transition to final state with effect"""
    instance = FinalInstance()

    async def transition_effect(ctx, inst, event):
        inst.log_action('transition-to-final')

    async def final_entry(ctx, inst, event):
        inst.log_action('final-entry')
        inst.data['finalized_at'] = time.time()

    model = hsm.define('FinalWithActionMachine',
        hsm.initial(hsm.target('active')),
        hsm.state('active',
            hsm.transition(
                hsm.on('end'),
                hsm.target('/finished'),
                hsm.effect(transition_effect)
            )
        ),
        hsm.final('finished')
    )

    ctx = hsm.Context()
    sm = await hsm.start(ctx, instance, model)

    # Transition to final state
    await sm.dispatch(Event('end'))

    assert instance.log == [
        'transition-to-final'
    ]
    assert sm.state() == '/FinalWithActionMachine/finished'

    await hsm.stop(sm)


@pytest.mark.asyncio
async def test_final_state_behavior_during_stop():
    """Final state behavior during stop"""
    instance = FinalInstance()

    async def normal_exit(ctx, inst, event):
        inst.log_action('normal-exit')

    model = hsm.define('FinalStopMachine',
        hsm.initial(hsm.target('normal')),
        hsm.state('normal',
            hsm.exit(normal_exit),
            hsm.transition(
                hsm.on('finish'),
                hsm.target('/terminal')
            )
        ),
        hsm.final('terminal')
    )

    ctx = hsm.Context()
    sm = await hsm.start(ctx, instance, model)

    # Move to final state
    await sm.dispatch(Event('finish'))
    assert sm.state() == '/FinalStopMachine/terminal'

    # Stop from final state
    instance.log = []
    await hsm.stop(sm)

    # Should not execute exit actions for final states
    assert instance.log == []


@pytest.mark.asyncio
async def test_transition_from_final_state_should_not_be_possible():
    """Transition from final state should not be possible"""
    instance = FinalInstance()

    model = hsm.define('FinalTransitionMachine',
        hsm.initial(hsm.target('start')),
        hsm.state('start',
            hsm.transition(
                hsm.on('end'),
                hsm.target('../final')
            )
        ),
        hsm.final('final')
    )

    ctx = hsm.Context()
    sm = await hsm.start(ctx, instance, model)

    # Go to final state
    await sm.dispatch(Event('end'))
    assert sm.state() == '/FinalTransitionMachine/final'

    # Try to send events - should be ignored
    await sm.dispatch(Event('restart'))
    await sm.dispatch(Event('anything'))

    # Should remain in final state
    assert sm.state() == '/FinalTransitionMachine/final'

    await hsm.stop(sm)


@pytest.mark.asyncio
async def test_complex_final_state_scenario_with_cleanup():
    """Complex final state scenario with cleanup"""
    instance = FinalInstance()

    async def workflow_cleanup(ctx, inst, event):
        inst.log_action('workflow-cleanup')

    async def resources_allocated(ctx, inst, event):
        inst.data['resources'] = ['resource1', 'resource2']
        inst.log_action('resources-allocated')

    async def processing_started(ctx, inst, event):
        inst.log_action('processing-started')

    async def cleaning_up(ctx, inst, event):
        inst.log_action('cleaning-up')
        inst.data['resources'] = []

    model = hsm.define('ComplexFinalMachine',
        hsm.initial(hsm.target('workflow')),
        hsm.state('workflow',
            hsm.initial(hsm.target('initialize')),
            hsm.exit(workflow_cleanup),
            hsm.state('initialize',
                hsm.entry(resources_allocated),
                hsm.transition(
                    hsm.on('proceed'),
                    hsm.target('../processing')
                )
            ),
            hsm.state('processing',
                hsm.entry(processing_started),
                hsm.transition(
                    hsm.on('complete'),
                    hsm.target('../cleanup')
                )
            ),
            hsm.state('cleanup',
                hsm.entry(cleaning_up),
                hsm.transition(
                    hsm.on('done'),
                    hsm.target('../finished')
                )
            ),
            hsm.final('finished')
        )
    )

    ctx = hsm.Context()
    sm = await hsm.start(ctx, instance, model)

    # Execute workflow
    await sm.dispatch(Event('proceed'))
    await sm.dispatch(Event('complete'))
    await sm.dispatch(Event('done'))

    assert instance.log == [
        'resources-allocated',
        'processing-started',
        'cleaning-up',
    ]
    assert sm.state() == '/ComplexFinalMachine/workflow/finished'
    assert instance.data['resources'] == []

    # Stop the machine
    await hsm.stop(sm)
    assert 'workflow-cleanup' in instance.log