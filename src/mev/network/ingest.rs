#![allow(dead_code)]

#[derive(Debug)]
pub struct XdpIngest {
    _private: (),
}

impl XdpIngest {
    pub fn new(interface: &str, queue_id: u32) -> Result<Self, Box<dyn std::error::Error>> {
        Err(format!(
            "AF_XDP ingest is not implemented for interface={interface} queue_id={queue_id}; use websocket mempool ingestion or add a real AF_XDP backend before enabling xdp-ingest"
        )
        .into())
    }

    pub async fn recv(&mut self) -> Option<Vec<u8>> {
        None
    }

    pub async fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        Err("AF_XDP ingest backend is not implemented".into())
    }

    pub fn backend_status() -> &'static str {
        "unimplemented"
    }
}

#[cfg(all(target_os = "linux", feature = "xdp-ingest"))]
mod linux_xdp {
    use io_uring::IoUring;

    pub struct LinuxXdpRing {
        pub ring: IoUring,
        pub socket_fd: std::os::fd::RawFd,
    }
}
