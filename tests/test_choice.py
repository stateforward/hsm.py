"""
Test choice pseudostates
Tests dynamic branching based on runtime conditions using choice pseudostates
"""

import pytest
from hsm import hsm
from hsm.hsm import Instance, Event, Context


class ChoiceInstance(Instance):
    def __init__(self):
        super().__init__()
        self.log = []
        self.data = {}

    def log_action(self, action):
        self.log.append(action)


@pytest.mark.asyncio
async def test_basic_choice_pseudostate_with_guards():
    """Basic choice pseudostate with guards"""
    instance = ChoiceInstance()
    instance.data['value'] = 5

    async def going_to_choice_effect(ctx, inst, event):
        inst.log_action('going-to-choice')

    async def chose_low_effect(ctx, inst, event):
        inst.log_action('chose-low')

    async def chose_medium_effect(ctx, inst, event):
        inst.log_action('chose-medium')

    async def chose_high_effect(ctx, inst, event):
        inst.log_action('chose-high')

    async def low_entry(ctx, inst, event):
        inst.log_action('low-entry')

    async def medium_entry(ctx, inst, event):
        inst.log_action('medium-entry')

    async def high_entry(ctx, inst, event):
        inst.log_action('high-entry')

    async def low_guard(ctx, inst, event):
        return inst.data['value'] < 3

    async def medium_guard(ctx, inst, event):
        return inst.data['value'] >= 3 and inst.data['value'] < 7

    model = hsm.define('BasicChoiceMachine',
        hsm.initial(hsm.target('start')),
        hsm.state('start',
            hsm.transition(
                hsm.on('decide'),
                hsm.target('../decision'),
                hsm.effect(going_to_choice_effect)
            )
        ),
        hsm.choice('decision',
            hsm.transition(
                hsm.guard(low_guard),
                hsm.target('low'),
                hsm.effect(chose_low_effect)
            ),
            hsm.transition(
                hsm.guard(medium_guard),
                hsm.target('medium'),
                hsm.effect(chose_medium_effect)
            ),
            hsm.transition(
                hsm.target('high'),
                hsm.effect(chose_high_effect)
            )
        ),
        hsm.state('low', hsm.entry(low_entry)),
        hsm.state('medium', hsm.entry(medium_entry)),
        hsm.state('high', hsm.entry(high_entry))
    )

    ctx = Context()
    sm = await hsm.start(ctx, instance, model)

    # Trigger choice evaluation
    await sm.dispatch(Event('decide'))

    # Should choose medium branch
    assert instance.log == [
        'going-to-choice',
        'chose-medium',
        'medium-entry'
    ]
    assert sm.state() == '/BasicChoiceMachine/medium'

    await hsm.stop(sm)


@pytest.mark.asyncio
async def test_choice_pseudostate_with_different_guard_outcomes():
    """Choice pseudostate with different guard outcomes"""
    instance = ChoiceInstance()

    test_cases = [
        {'value': 1, 'expected_state': 'low', 'expected_effect': 'chose-low'},
        {'value': 5, 'expected_state': 'medium', 'expected_effect': 'chose-medium'},
        {'value': 10, 'expected_state': 'high', 'expected_effect': 'chose-high'}
    ]

    for test_case in test_cases:
        instance.data['value'] = test_case['value']
        instance.log = []

        async def low_guard(ctx, inst, event):
            return inst.data['value'] < 3

        async def medium_guard(ctx, inst, event):
            return inst.data['value'] >= 3 and inst.data['value'] < 7

        async def chose_low_effect(ctx, inst, event):
            inst.log_action('chose-low')

        async def chose_medium_effect(ctx, inst, event):
            inst.log_action('chose-medium')

        async def chose_high_effect(ctx, inst, event):
            inst.log_action('chose-high')

        model = hsm.define('ChoiceTestMachine',
            hsm.initial(hsm.target('choice')),
            hsm.choice('choice',
                hsm.transition(
                    hsm.guard(low_guard),
                    hsm.target('low'),
                    hsm.effect(chose_low_effect)
                ),
                hsm.transition(
                    hsm.guard(medium_guard),
                    hsm.target('medium'),
                    hsm.effect(chose_medium_effect)
                ),
                hsm.transition(
                    hsm.target('high'),
                    hsm.effect(chose_high_effect)
                )
            ),
            hsm.state('low'),
            hsm.state('medium'),
            hsm.state('high')
        )

        ctx = Context()
        sm = await hsm.start(ctx, instance, model)

        assert test_case['expected_effect'] in instance.log
        assert sm.state() == f'/ChoiceTestMachine/{test_case["expected_state"]}'

        await hsm.stop(sm)


@pytest.mark.asyncio
async def test_choice_in_hierarchical_state():
    """Choice in hierarchical state"""
    instance = ChoiceInstance()
    instance.data['direction'] = 'left'

    async def left_guard(ctx, inst, event):
        return inst.data['direction'] == 'left'

    async def right_guard(ctx, inst, event):
        return inst.data['direction'] == 'right'

    async def routed_left_effect(ctx, inst, event):
        inst.log_action('routed-left')

    async def routed_right_effect(ctx, inst, event):
        inst.log_action('routed-right')

    async def routed_center_effect(ctx, inst, event):
        inst.log_action('routed-center')

    async def left_entry(ctx, inst, event):
        inst.log_action('left-entry')

    async def right_entry(ctx, inst, event):
        inst.log_action('right-entry')

    async def center_entry(ctx, inst, event):
        inst.log_action('center-entry')

    model = hsm.define('HierarchicalChoiceMachine',
        hsm.initial(hsm.target('container')),
        hsm.state('container',
            hsm.initial(hsm.target('router')),
            hsm.choice('router',
                hsm.transition(
                    hsm.guard(left_guard),
                    hsm.target('left'),
                    hsm.effect(routed_left_effect)
                ),
                hsm.transition(
                    hsm.guard(right_guard),
                    hsm.target('right'),
                    hsm.effect(routed_right_effect)
                ),
                hsm.transition(
                    hsm.target('center'),
                    hsm.effect(routed_center_effect)
                )
            ),
            hsm.state('left', hsm.entry(left_entry)),
            hsm.state('right', hsm.entry(right_entry)),
            hsm.state('center', hsm.entry(center_entry))
        )
    )

    ctx = Context()
    sm = await hsm.start(ctx, instance, model)

    assert instance.log == [
        'routed-left',
        'left-entry'
    ]
    assert sm.state() == '/HierarchicalChoiceMachine/container/left'

    await hsm.stop(sm)


@pytest.mark.asyncio
async def test_choice_with_complex_guard_conditions():
    """Choice with complex guard conditions"""
    instance = ChoiceInstance()
    instance.data['config'] = {
        'enabled': True,
        'priority': 2,
        'mode': 'auto'
    }

    async def disabled_guard(ctx, inst, event):
        cfg = inst.data['config']
        return not cfg['enabled']

    async def high_priority_guard(ctx, inst, event):
        cfg = inst.data['config']
        return cfg['enabled'] and cfg['priority'] > 5 and cfg['mode'] == 'manual'

    async def auto_guard(ctx, inst, event):
        cfg = inst.data['config']
        return cfg['enabled'] and cfg['mode'] == 'auto'

    async def disabled_effect(ctx, inst, event):
        inst.log_action('disabled-path')

    async def high_priority_effect(ctx, inst, event):
        inst.log_action('high-priority-manual')

    async def automatic_effect(ctx, inst, event):
        inst.log_action('automatic-mode')

    async def default_effect(ctx, inst, event):
        inst.log_action('default-fallback')

    model = hsm.define('ComplexChoiceMachine',
        hsm.initial(hsm.target('choice')),
        hsm.choice('choice',
            hsm.transition(
                hsm.guard(disabled_guard),
                hsm.target('disabled'),
                hsm.effect(disabled_effect)
            ),
            hsm.transition(
                hsm.guard(high_priority_guard),
                hsm.target('highpriority'),
                hsm.effect(high_priority_effect)
            ),
            hsm.transition(
                hsm.guard(auto_guard),
                hsm.target('automatic'),
                hsm.effect(automatic_effect)
            ),
            hsm.transition(
                hsm.target('default'),
                hsm.effect(default_effect)
            )
        ),
        hsm.state('disabled'),
        hsm.state('highpriority'),
        hsm.state('automatic'),
        hsm.state('default')
    )

    ctx = Context()
    sm = await hsm.start(ctx, instance, model)

    # Should choose automatic mode
    assert instance.log == ['automatic-mode']
    assert sm.state() == '/ComplexChoiceMachine/automatic'

    await hsm.stop(sm)


@pytest.mark.asyncio
async def test_choice_with_event_data_evaluation():
    """Choice with event data evaluation"""
    instance = ChoiceInstance()

    async def urgent_guard(ctx, inst, event):
        return hasattr(event, 'data') and event.data and event.data.get('type') == 'urgent'

    async def normal_guard(ctx, inst, event):
        return hasattr(event, 'data') and event.data and event.data.get('type') == 'normal'

    async def urgent_effect(ctx, inst, event):
        inst.log_action('urgent-processing')

    async def normal_effect(ctx, inst, event):
        inst.log_action('normal-processing')

    async def fallback_effect(ctx, inst, event):
        inst.log_action('fallback-processing')

    model = hsm.define('EventChoiceMachine',
        hsm.initial(hsm.target('waiting')),
        hsm.state('waiting',
            hsm.transition(
                hsm.on('process'),
                hsm.target('../router')
            )
        ),
        hsm.choice('router',
            hsm.transition(
                hsm.guard(urgent_guard),
                hsm.target('urgent'),
                hsm.effect(urgent_effect)
            ),
            hsm.transition(
                hsm.guard(normal_guard),
                hsm.target('normal'),
                hsm.effect(normal_effect)
            ),
            hsm.transition(
                hsm.target('fallback'),
                hsm.effect(fallback_effect)
            )
        ),
        hsm.state('urgent'),
        hsm.state('normal'),
        hsm.state('fallback')
    )

    ctx = Context()
    sm = await hsm.start(ctx, instance, model)

    # Send event with urgent data
    event = Event('process', {'type': 'urgent', 'priority': 1})
    await sm.dispatch(event)

    assert instance.log == ['urgent-processing']
    assert sm.state() == '/EventChoiceMachine/urgent'

    await hsm.stop(sm)


@pytest.mark.asyncio
async def test_choice_with_no_matching_guards_should_raise_error():
    """Choice with no matching guards - should raise error"""
    instance = ChoiceInstance()

    async def always_false_guard(ctx, inst, event):
        return False

    # Should raise validation error at definition time since choice lacks guardless fallback
    with pytest.raises(hsm.ValidationError, match="the last transition of choice state.*cannot have a guard"):
        model = hsm.define('NoMatchChoiceMachine',
            hsm.initial(hsm.target('choice')),
            hsm.choice('choice',
                hsm.transition(
                    hsm.guard(always_false_guard),
                    hsm.target('never')
                )
            ),
            hsm.state('never')
        )


@pytest.mark.asyncio
async def test_nested_choice_pseudostates():
    """Nested choice pseudostates"""
    instance = ChoiceInstance()
    instance.data['level1'] = True
    instance.data['level2'] = 'b'

    async def level1_guard(ctx, inst, event):
        return inst.data['level1']

    async def level2a_guard(ctx, inst, event):
        return inst.data['level2'] == 'a'

    async def level2b_guard(ctx, inst, event):
        return inst.data['level2'] == 'b'

    async def level1_true_effect(ctx, inst, event):
        inst.log_action('level1-true')

    async def level1_false_effect(ctx, inst, event):
        inst.log_action('level1-false')

    async def level2a_effect(ctx, inst, event):
        inst.log_action('level2-a')

    async def level2b_effect(ctx, inst, event):
        inst.log_action('level2-b')

    async def level2_other_effect(ctx, inst, event):
        inst.log_action('level2-other')

    model = hsm.define('NestedChoiceMachine',
        hsm.initial(hsm.target('level1choice')),
        hsm.choice('level1choice',
            hsm.transition(
                hsm.guard(level1_guard),
                hsm.target('level2choice'),
                hsm.effect(level1_true_effect)
            ),
            hsm.transition(
                hsm.target('level1false'),
                hsm.effect(level1_false_effect)
            )
        ),
        hsm.choice('level2choice',
            hsm.transition(
                hsm.guard(level2a_guard),
                hsm.target('result_a'),
                hsm.effect(level2a_effect)
            ),
            hsm.transition(
                hsm.guard(level2b_guard),
                hsm.target('result_b'),
                hsm.effect(level2b_effect)
            ),
            hsm.transition(
                hsm.target('result_other'),
                hsm.effect(level2_other_effect)
            )
        ),
        hsm.state('level1false'),
        hsm.state('result_a'),
        hsm.state('result_b'),
        hsm.state('result_other')
    )

    ctx = Context()
    sm = await hsm.start(ctx, instance, model)

    # Should follow level1 choice then level2 choice
    assert instance.log == [
        'level1-true',
        'level2-b'
    ]
    assert sm.state() == '/NestedChoiceMachine/result_b'

    await hsm.stop(sm)


@pytest.mark.asyncio
async def test_choice_guard_evaluation_order():
    """Choice with side effects in guards"""
    instance = ChoiceInstance()

    async def guard1(ctx, inst, event):
        inst.log_action('guard1-evaluated')
        return False

    async def guard2(ctx, inst, event):
        inst.log_action('guard2-evaluated')
        return True

    async def guard3(ctx, inst, event):
        inst.log_action('guard3-evaluated')
        return True

    async def path2_effect(ctx, inst, event):
        inst.log_action('path2-effect')

    model = hsm.define('SideEffectChoiceMachine',
        hsm.initial(hsm.target('choice')),
        hsm.choice('choice',
            hsm.transition(
                hsm.guard(guard1),
                hsm.target('path1')
            ),
            hsm.transition(
                hsm.guard(guard2),
                hsm.target('path2'),
                hsm.effect(path2_effect)
            ),
            hsm.transition(
                hsm.target('path3')
            )
        ),
        hsm.state('path1'),
        hsm.state('path2'),
        hsm.state('path3')
    )

    ctx = Context()
    sm = await hsm.start(ctx, instance, model)

    # Should evaluate guards in order until first true
    assert instance.log == [
        'guard1-evaluated',
        'guard2-evaluated',
        'path2-effect'
    ]
    assert sm.state() == '/SideEffectChoiceMachine/path2'

    await hsm.stop(sm)