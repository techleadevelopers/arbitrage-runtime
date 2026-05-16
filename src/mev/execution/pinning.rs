#[derive(Clone, Copy, Debug)]
pub struct ThreadPinningConfig {
    pub network_core: usize,
    pub decode_core: usize,
    pub eval_core: usize,
    pub exec_core: usize,
    pub numa_node: usize,
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
        let core_count = std::thread::available_parallelism()
            .map(|value| value.get())
            .unwrap_or(1)
            .max(1);
        Self {
            network_core: 0,
            decode_core: 1 % core_count,
            eval_core: 2 % core_count,
            exec_core: 3 % core_count,
            numa_node: 0,
        }
    }

    pub fn pin_current_thread(&self, role: ThreadRole) -> bool {
        let core_id = match role {
            ThreadRole::Network => self.network_core,
            ThreadRole::Decode => self.decode_core,
            ThreadRole::Eval => self.eval_core,
            ThreadRole::Executor => self.exec_core,
        };

        core_affinity::set_for_current(core_affinity::CoreId { id: core_id })
    }
}
