//! One coherent, revisioned projection of actual native service transitions.
//!
//! Mutating owners publish while holding their own lock, then this cell. Readers
//! take only this cell; publication takes only the existing event owner. Neither
//! path calls back into control, output, process discovery or terminal export.
use std::sync::{Arc, Mutex, OnceLock, Weak};

use ctxmux_protocol::{
    NativeInputPhase, NativeInputStatus, NativeOutputStatus, NativeOwnerStatus,
    NativeServiceFailure, NativeServiceSnapshot, NativeTerminalFault, RunEvent,
};

use crate::{Run, mutex_lock};

#[derive(Clone)]
pub(crate) struct NativeService(Arc<ServiceInner>);

struct ServiceInner {
    snapshot: Mutex<NativeServiceSnapshot>,
    run: OnceLock<Weak<Run>>,
}

impl NativeService {
    pub(crate) fn new(historical: bool) -> Self {
        Self(Arc::new(ServiceInner {
            snapshot: Mutex::new(NativeServiceSnapshot {
                revision: 0,
                owner: if historical {
                    NativeOwnerStatus::Stopped {
                        reason: NativeServiceFailure::Historical,
                    }
                } else {
                    NativeOwnerStatus::Starting {}
                },
                output: if historical {
                    NativeOutputStatus::Closed {}
                } else {
                    NativeOutputStatus::Pending {}
                },
                input: NativeInputStatus {
                    phase: if historical {
                        NativeInputPhase::Closed {}
                    } else {
                        NativeInputPhase::Open {}
                    },
                    unsettled_commands: 0,
                    unsettled_request_bytes: 0,
                    write_blocked: false,
                    completed_input_bytes: (!historical).then_some(0),
                    active_confirmed_bytes: 0,
                    current_size: None,
                },
                terminal_fault: None,
            }),
            run: OnceLock::new(),
        }))
    }

    pub(crate) fn bind(&self, run: &Arc<Run>) {
        assert!(
            self.0.run.set(Arc::downgrade(run)).is_ok(),
            "one native service owner per Run"
        );
    }

    pub(crate) fn snapshot(&self) -> NativeServiceSnapshot {
        mutex_lock(&self.0.snapshot).clone()
    }

    pub(crate) fn update_input(&self, input: NativeInputStatus) {
        self.update(|snapshot| {
            // The sole input owner establishes this failure fence while its
            // state lock excludes future writes. Publish shared-owner loss and
            // settled input facts together, rather than a Serving owner beside
            // a lane that has already lost that same owner.
            if let NativeInputPhase::Unavailable { reason } = &input.phase
                && matches!(
                    reason,
                    NativeServiceFailure::OwnerStopped
                        | NativeServiceFailure::OwnerUnwound
                        | NativeServiceFailure::OwnerIoFailed { .. }
                )
            {
                snapshot.owner = NativeOwnerStatus::Stopped { reason: *reason };
                if !matches!(snapshot.output, NativeOutputStatus::Closed {}) {
                    snapshot.output = NativeOutputStatus::Unavailable { reason: *reason };
                }
            }
            snapshot.input = input;
        });
    }

    pub(crate) fn update_output(&self, output: NativeOutputStatus) {
        self.update(|snapshot| snapshot.output = output);
    }

    pub(crate) fn ready(&self) {
        self.update(|snapshot| {
            snapshot.owner = NativeOwnerStatus::Serving {};
            snapshot.output = NativeOutputStatus::Serving {};
        });
    }

    pub(crate) fn draining(&self) {
        self.update(|snapshot| snapshot.owner = NativeOwnerStatus::Draining {});
    }

    pub(crate) fn stopped(&self, reason: NativeServiceFailure) {
        self.update(|snapshot| {
            snapshot.owner = NativeOwnerStatus::Stopped { reason };
            if !matches!(snapshot.output, NativeOutputStatus::Closed {}) {
                snapshot.output = NativeOutputStatus::Unavailable { reason };
            }
            // Availability changes immediately; unsettled charges and confirmed
            // prefixes remain actual facts until the control owner settles them.
            if !matches!(snapshot.input.phase, NativeInputPhase::Closed {}) {
                snapshot.input.phase = NativeInputPhase::Unavailable { reason };
                snapshot.input.write_blocked = false;
            }
        });
    }

    pub(crate) fn terminal_fault(&self, fault: NativeTerminalFault) {
        self.update(|snapshot| {
            if snapshot.terminal_fault.is_none() {
                snapshot.terminal_fault = Some(fault);
            }
        });
    }

    fn update(&self, update: impl FnOnce(&mut NativeServiceSnapshot)) {
        let mut snapshot = mutex_lock(&self.0.snapshot);
        let before = snapshot.clone();
        update(&mut snapshot);
        if *snapshot == before {
            return;
        }
        snapshot.revision = before
            .revision
            .checked_add(1)
            .expect("native service revision remains representable");
        // Keep the event ordered with the transition. No other owner mutex is
        // acquired by publish_event, and snapshot never acquires an owner lock.
        if let Some(run) = self.0.run.get().and_then(Weak::upgrade) {
            run.publish_event(RunEvent::ServiceChanged {
                service: snapshot.clone(),
            });
        }
    }
}

/// Both Arc allocations are charged as fixed native metadata, independently of
/// the event ring's existing leases. Weak binding never keeps a Run alive.
pub(crate) const fn resident_service_owner_bytes() -> usize {
    std::mem::size_of::<ServiceInner>() + 2 * std::mem::size_of::<usize>()
}
