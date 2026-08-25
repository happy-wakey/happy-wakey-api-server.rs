use happy_wakey_interfaces::{
    AlarmOccurrenceState as State, AlarmTransitionEvent as Event, TransitionDisposition,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Decision {
    pub disposition: TransitionDisposition,
    pub next: State,
}

/// Total, deterministic authority for occurrence transitions.
pub fn decide(
    current: State,
    event: Event,
    actual_generation: u64,
    expected_generation: u64,
    has_snooze_deadline: bool,
) -> Decision {
    if actual_generation != expected_generation {
        return Decision {
            disposition: TransitionDisposition::Stale,
            next: current,
        };
    }
    use Event::*;
    use State::*;
    let next = match (current, event, has_snooze_deadline) {
        (Scheduled, Fire, _) => Some(Firing),
        (Scheduled, Cancel, _) => Some(Canceled),
        (Scheduled, MarkMissed, _) => Some(Missed),
        (Firing, Acknowledge, _) => Some(Acknowledged),
        (Firing | Acknowledged, Snooze, true) => Some(Snoozed),
        (Firing, MarkMissed, _) => Some(Missed),
        (Firing | Acknowledged, Cancel, _) => Some(Canceled),
        (Acknowledged, Complete, _) => Some(Completed),
        (Snoozed, Fire, _) => Some(Firing),
        (Snoozed, Cancel, _) => Some(Canceled),
        _ => None,
    };
    match next {
        Some(next) => Decision {
            disposition: TransitionDisposition::Applied,
            next,
        },
        None => Decision {
            disposition: TransitionDisposition::Rejected,
            next: current,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const STATES: [State; 7] = [
        State::Scheduled,
        State::Firing,
        State::Acknowledged,
        State::Snoozed,
        State::Completed,
        State::Missed,
        State::Canceled,
    ];
    const EVENTS: [Event; 6] = [
        Event::Fire,
        Event::Acknowledge,
        Event::Snooze,
        Event::Complete,
        Event::MarkMissed,
        Event::Cancel,
    ];

    #[test]
    fn every_pair_is_classified_and_terminal_states_are_closed() {
        for state in STATES {
            for event in EVENTS {
                let decision = decide(state, event, 4, 4, true);
                assert!(matches!(
                    decision.disposition,
                    TransitionDisposition::Applied | TransitionDisposition::Rejected
                ));
                if matches!(state, State::Completed | State::Missed | State::Canceled) {
                    assert_eq!(decision.disposition, TransitionDisposition::Rejected);
                    assert_eq!(decision.next, state);
                }
            }
        }
    }

    #[test]
    fn stale_generation_never_mutates() {
        for state in STATES {
            for event in EVENTS {
                let decision = decide(state, event, 4, 3, true);
                assert_eq!(decision.disposition, TransitionDisposition::Stale);
                assert_eq!(decision.next, state);
            }
        }
    }

    #[test]
    fn snooze_requires_a_deadline() {
        assert_eq!(
            decide(State::Firing, Event::Snooze, 1, 1, false).disposition,
            TransitionDisposition::Rejected
        );
    }
}
