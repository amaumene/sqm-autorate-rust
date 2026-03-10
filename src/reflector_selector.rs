use crate::util::{MutexExt, RwLockExt};
use crate::{Config, ReflectorStats};
use log::{debug, info};
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::mpsc::Receiver;
use std::sync::{Arc, Mutex, RwLock};
use std::thread::sleep;
use std::time::Duration;

pub struct ReflectorSelector {
    pub config: Config,
    pub owd_recent: Arc<Mutex<HashMap<IpAddr, ReflectorStats>>>,
    pub reflector_peers_lock: Arc<RwLock<Vec<IpAddr>>>,
    pub reflector_pool: Vec<IpAddr>,
    pub trigger_channel: Receiver<bool>,
}

impl ReflectorSelector {
    pub fn run(&self) -> anyhow::Result<()> {
        let mut selector_sleep_time = Duration::new(30, 0);
        let mut reselection_count = 0;
        let baseline_sleep_time =
            Duration::from_secs_f64(self.config.tick_interval * std::f64::consts::PI);

        // Initial wait of several seconds to allow some OWD data to build up
        sleep(baseline_sleep_time);

        loop {
            /*
             * Selection is triggered either by some other thread triggering it through the channel,
             * or it passes the timeout. In any case we don't care about the result of this function,
             * so we ignore the result of it.
             */
            let _ = self
                .trigger_channel
                .recv_timeout(selector_sleep_time)
                .unwrap_or(true);
            reselection_count += 1;
            info!("Starting reselection [#{}]", reselection_count);

            // After 40 reselections, slow down to every 15 minutes
            if reselection_count > 40 {
                selector_sleep_time = Duration::new(self.config.peer_reselection_time * 60, 0);
            }

            let mut next_peers: Vec<IpAddr> = Vec::new();
            let mut reflectors_peers = self.reflector_peers_lock.write_anyhow()?;

            // Include all current peers
            for reflector in reflectors_peers.iter() {
                debug!("Current peer: {}", reflector);
                next_peers.push(*reflector);
            }

            // Pick new candidates from the pool via a partial Fisher-Yates shuffle.
            // This guarantees we get up to `want` unique candidates without retries.
            let want = 20.min(self.reflector_pool.len());
            let mut pool = self.reflector_pool.clone();
            for i in 0..want {
                let j = fastrand::usize(i..pool.len());
                pool.swap(i, j);
                if !next_peers.contains(&pool[i]) {
                    debug!("Next candidate: {}", pool[i]);
                    next_peers.push(pool[i]);
                }
            }

            // Clone next_peers because we need it again after the baseline sleep
            // to iterate over candidates for RTT measurement.
            *reflectors_peers = next_peers.clone();

            // Drop the write guard explicitly so other threads can read
            // while we wait for baselines
            drop(reflectors_peers);

            debug!("Waiting for candidates to be baselined");
            // Wait for several seconds to allow all reflectors to be re-baselined
            sleep(baseline_sleep_time);

            // Re-acquire the lock when we wake up again
            reflectors_peers = self.reflector_peers_lock.write_anyhow()?;

            let mut candidates = Vec::new();
            let owd_recent = self.owd_recent.lock_anyhow()?;

            for peer in next_peers {
                if owd_recent.contains_key(&peer) {
                    let rtt = (owd_recent[&peer].down_ewma + owd_recent[&peer].up_ewma) as u64;
                    candidates.push((peer, rtt));
                    info!("Candidate reflector: {} RTT: {}", peer.to_string(), rtt);
                } else {
                    info!(
                        "No data found from candidate reflector: {} - skipping",
                        peer.to_string()
                    );
                }
            }

            // Sort the candidates table now by ascending RTT
            candidates.sort_by(|a, b| a.1.cmp(&b.1));

            // Limit candidates down to 2 * num_reflectors
            let num_reflectors = self.config.num_reflectors as usize;
            let candidate_pool_num = num_reflectors.saturating_mul(2);
            candidates.truncate(candidate_pool_num);

            for (candidate, rtt) in candidates.iter() {
                info!("Fastest candidate {}: {}", candidate, rtt);
            }

            // Shuffle the deck so we avoid overwhelming good reflectors (Fisher-Yates)
            for i in (1_usize..candidates.len()).rev() {
                let j = fastrand::usize(0..=i);
                candidates.swap(i, j);
            }

            let take = num_reflectors.min(candidates.len());
            let mut new_peers = Vec::with_capacity(take);
            for (peer, _) in &candidates[..take] {
                new_peers.push(*peer);
                info!("New selected peer: {}", peer);
            }

            *reflectors_peers = new_peers;
        }
    }
}
