#![cfg_attr(not(target_os = "linux"), allow(dead_code))]

#[derive(Clone, Copy, Debug)]
pub struct ThreadPinningConfig {
    pub network_core: usize,
    pub decode_core: usize,
    pub eval_core: usize,
    pub exec_core: usize,
    pub numa_node: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct PinResult {
    pub requested_core: usize,
    pub current_core: Option<usize>,
    pub numa_node: usize,
    pub success: bool,
}

#[derive(Clone, Copy, Debug)]
pub enum ThreadRole {
    Network,
    Decode,
    Eval,
    Executor,
}

impl ThreadPinningConfig {
    pub fn auto_detect() -> Self {
        let detected: Vec<usize> = core_affinity::get_core_ids()
            .unwrap_or_default()
            .into_iter()
            .map(|core| core.id)
            .collect();
        let fallback_count = std::thread::available_parallelism()
            .map(|value| value.get())
            .unwrap_or(1)
            .max(1);
        let cores = if detected.is_empty() {
            (0..fallback_count).collect::<Vec<_>>()
        } else {
            detected
        };
        let pick = |index: usize| cores[index % cores.len()];
        Self {
            network_core: pick(0),
            decode_core: pick(1),
            eval_core: pick(2),
            exec_core: pick(3),
            numa_node: 0,
        }
    }

    pub fn pin_current_thread(&self, role: ThreadRole) -> PinResult {
        let requested_core = match role {
            ThreadRole::Network => self.network_core,
            ThreadRole::Decode => self.decode_core,
            ThreadRole::Eval => self.eval_core,
            ThreadRole::Executor => self.exec_core,
        };

        let success = core_affinity::set_for_current(core_affinity::CoreId { id: requested_core });
        PinResult {
            requested_core,
            current_core: current_cpu_core_id(),
            numa_node: self.numa_node,
            success,
        }
    }

    pub fn pin_runtime_load_worker(&self, worker_id: usize) -> PinResult {
        let core_ids: Vec<usize> = core_affinity::get_core_ids()
            .unwrap_or_default()
            .into_iter()
            .map(|core| core.id)
            .collect();
        let requested_core = if core_ids.is_empty() {
            self.eval_core
        } else {
            core_ids[worker_id % core_ids.len()]
        };
        let success = core_affinity::set_for_current(core_affinity::CoreId { id: requested_core });
        PinResult {
            requested_core,
            current_core: current_cpu_core_id(),
            numa_node: self.numa_node,
            success,
        }
    }
}

#[cfg(target_os = "linux")]
fn current_cpu_core_id() -> Option<usize> {
    let current = unsafe { libc::sched_getcpu() };
    (current >= 0).then_some(current as usize)
}

#[cfg(not(target_os = "linux"))]
fn current_cpu_core_id() -> Option<usize> {
    None
}
