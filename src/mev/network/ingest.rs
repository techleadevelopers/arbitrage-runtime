#![allow(dead_code)]

use tokio::sync::mpsc;

#[derive(Debug)]
pub struct XdpIngest {
    rx: mpsc::UnboundedReceiver<Vec<u8>>,
    tx: mpsc::UnboundedSender<Vec<u8>>,
}

impl XdpIngest {
    pub fn new(_interface: &str, _queue_id: u32) -> Result<Self, Box<dyn std::error::Error>> {
        let (tx, rx) = mpsc::unbounded_channel();
        Ok(Self { rx, tx })
    }

    pub fn sender(&self) -> mpsc::UnboundedSender<Vec<u8>> {
        self.tx.clone()
    }

    pub async fn recv(&mut self) -> Option<Vec<u8>> {
        self.rx.recv().await
    }

    pub async fn run(&mut self) {
        while self.rx.recv().await.is_some() {}
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
