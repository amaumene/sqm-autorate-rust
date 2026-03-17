use crate::netlink::{Netlink, NetlinkError, Qdisc};
use crate::time::Time;
use crate::util::{MutexExt, RwLockExt};
use crate::{Config, ReflectorStats};
use log::{debug, info, warn};
use rustix::thread::ClockId;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Seek, Write};
use std::net::IpAddr;
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex, RwLock};
use std::thread::sleep;
use std::time::{Duration, Instant};
use thiserror::Error;

#[derive(Copy, Clone, Debug, PartialEq)]
enum Direction {
    Down,
    Up,
}

#[derive(Debug, Error)]
pub enum RatecontrolError {
    #[error("Netlink error")]
    Netlink(#[from] NetlinkError),
}

#[derive(Copy, Clone, Debug)]
pub enum StatsDirection {
    RX,
    TX,
}

fn generate_initial_speeds(min_speed: f64, size: u32) -> Vec<f64> {
    vec![min_speed; size as usize]
}

fn get_interface_stats(
    config: &Config,
    down_direction: StatsDirection,
    up_direction: StatsDirection,
) -> Result<(i128, i128), RatecontrolError> {
    let (down_rx, down_tx) = Netlink::get_interface_stats(config.download_interface.as_str())?;
    let (up_rx, up_tx) = Netlink::get_interface_stats(config.upload_interface.as_str())?;

    let rx_bytes = match down_direction {
        StatsDirection::RX => down_rx,
        StatsDirection::TX => down_tx,
    };

    let tx_bytes = match up_direction {
        StatsDirection::RX => up_rx,
        StatsDirection::TX => up_tx,
    };

    Ok((rx_bytes.into(), tx_bytes.into()))
}

#[derive(Clone, Debug)]
struct State {
    current_bytes: i128,
    current_rate: f64,
    delta_stat: f64,
    deltas: Vec<f64>,
    qdisc: Qdisc,
    load: f64,
    next_rate: f64,
    nrate: usize,
    previous_bytes: i128,
    prev_t: Instant,
    safe_rates: Vec<f64>,
    utilisation: f64,
}

impl State {
    fn new(qdisc: Qdisc, previous_bytes: i128, safe_rates: Vec<f64>) -> Self {
        State {
            current_bytes: 0,
            current_rate: 0.0,
            delta_stat: 0.0,
            deltas: Vec::new(),
            load: 0.0,
            next_rate: 0.0,
            nrate: 0,
            qdisc,
            previous_bytes,
            prev_t: Instant::now(),
            safe_rates,
            utilisation: 0.0,
        }
    }
}

pub struct Ratecontroller {
    config: Config,
    down_direction: StatsDirection,
    owd_baseline: Arc<Mutex<HashMap<IpAddr, ReflectorStats>>>,
    owd_recent: Arc<Mutex<HashMap<IpAddr, ReflectorStats>>>,
    reflectors_lock: Arc<RwLock<Vec<IpAddr>>>,
    reselect_trigger: Sender<bool>,
    state_dl: State,
    state_ul: State,
    up_direction: StatsDirection,
}

impl Ratecontroller {
    fn calculate_rate(&mut self, direction: Direction) -> anyhow::Result<()> {
        let (base_rate, delay_ms, min_rate, state) = if direction == Direction::Down {
            (
                self.config.download_base_kbits,
                self.config.download_delay_ms,
                self.config.download_min_kbits,
                &mut self.state_dl,
            )
        } else {
            (
                self.config.upload_base_kbits,
                self.config.upload_delay_ms,
                self.config.upload_min_kbits,
                &mut self.state_ul,
            )
        };

        let now_t = Instant::now();
        let dur = now_t.duration_since(state.prev_t);

        if !state.deltas.is_empty() {
            state.next_rate = state.current_rate;

            state.delta_stat = state.deltas[state.deltas.len() / 2];

            if state.delta_stat > 0.0 {
                /*
                 * TODO - find where the (8 / 1000) comes from and
                 *    i. convert to a pre-computed factor
                 *    ii. ideally, see if it can be defined in terms of constants, eg ticks per second and number of active reflectors
                 */
                state.utilisation = (8.0 / 1000.0)
                    * (state.current_bytes as f64 - state.previous_bytes as f64)
                    / dur.as_secs_f64();
                state.load = state.utilisation / state.current_rate;

                // only increase when delay is well below threshold —
                // the zone between 30% and 100% of delay_ms is a hold zone
                // where we don't increase into growing latency
                if state.delta_stat < delay_ms * 0.3
                    && state.load > self.config.high_load_level
                {
                    // cap safe_rates at base_rate so load > 1.0 bursts
                    // can't create a positive feedback loop
                    state.safe_rates[state.nrate] =
                        (state.current_rate * state.load).floor().min(base_rate);
                    let max_rate = state
                        .safe_rates
                        .iter()
                        .max_by(|a, b| a.total_cmp(b))
                        .unwrap();
                    state.next_rate = state.current_rate
                        * (1.0 + 0.1 * (1.0_f64 - state.current_rate / max_rate).max(0.0))
                        + (base_rate * 0.03);
                    state.nrate += 1;
                    state.nrate %= self.config.speed_hist_size as usize;
                }

                if state.delta_stat > delay_ms {
                    let rnd_rate =
                        state.safe_rates[fastrand::usize(..state.safe_rates.len())];
                    state.next_rate =
                        rnd_rate.min(0.9 * state.current_rate * state.load);
                }
            }
        }

        // clamp between min and base_rate — going above base is pointless,
        // CAKE can't enforce a rate higher than the physical link
        state.next_rate = state.next_rate.clamp(min_rate, base_rate).floor();
        state.previous_bytes = state.current_bytes;
        state.prev_t = now_t;

        Ok(())
    }

    fn update_deltas(&mut self) -> anyhow::Result<()> {
        let state_dl = &mut self.state_dl;
        let state_ul = &mut self.state_ul;

        state_dl.deltas.clear();
        state_ul.deltas.clear();

        let now_t = Instant::now();
        let owd_baseline = self.owd_baseline.lock_anyhow()?;
        let owd_recent = self.owd_recent.lock_anyhow()?;
        let reflectors = self.reflectors_lock.read_anyhow()?;

        for reflector in reflectors.iter() {
            // only consider this data if it's less than 2 * tick_duration seconds old
            if owd_baseline.contains_key(reflector)
                && owd_recent.contains_key(reflector)
                && now_t
                    .duration_since(owd_recent[reflector].last_receive_time_s)
                    .as_secs_f64()
                    < self.config.tick_interval * 2.0
            {
                state_dl
                    .deltas
                    .push(owd_recent[reflector].down_ewma - owd_baseline[reflector].down_ewma);
                state_ul
                    .deltas
                    .push(owd_recent[reflector].up_ewma - owd_baseline[reflector].up_ewma);

                debug!(
                    "Reflector: {} down_delay: {} up_delay: {}",
                    reflector,
                    state_dl.deltas.last().unwrap(),
                    state_ul.deltas.last().unwrap()
                );
            }
        }

        // sort owd's lowest to highest
        state_dl.deltas.sort_by(|a, b| a.total_cmp(b));
        state_ul.deltas.sort_by(|a, b| a.total_cmp(b));

        if state_dl.deltas.len() < 5 || state_ul.deltas.len() < 5 {
            // trigger reselection
            warn!("Not enough delta values, triggering reselection");
            let _ = self.reselect_trigger.send(true);
        }

        Ok(())
    }

    pub fn new(
        config: Config,
        owd_baseline: Arc<Mutex<HashMap<IpAddr, ReflectorStats>>>,
        owd_recent: Arc<Mutex<HashMap<IpAddr, ReflectorStats>>>,
        reflectors_lock: Arc<RwLock<Vec<IpAddr>>>,
        reselect_trigger: Sender<bool>,
        down_direction: StatsDirection,
        up_direction: StatsDirection,
    ) -> anyhow::Result<Self> {
        let dl_qdisc = Netlink::qdisc_from_ifname(config.download_interface.as_str())?;
        let dl_safe_rates =
            generate_initial_speeds(config.download_min_kbits, config.speed_hist_size);
        let ul_qdisc = Netlink::qdisc_from_ifname(config.upload_interface.as_str())?;
        let ul_safe_rates =
            generate_initial_speeds(config.upload_min_kbits, config.speed_hist_size);

        let (cur_rx, cur_tx) = get_interface_stats(&config, down_direction, up_direction)?;

        Ok(Self {
            config,
            down_direction,
            owd_baseline,
            owd_recent,
            reflectors_lock,
            reselect_trigger,
            state_dl: State::new(dl_qdisc, cur_rx, dl_safe_rates),
            state_ul: State::new(ul_qdisc, cur_tx, ul_safe_rates),
            up_direction,
        })
    }

    pub fn run(&mut self) -> anyhow::Result<()> {
        let sleep_time = Duration::from_secs_f64(self.config.min_change_interval);

        let mut lastchg_t = Instant::now();
        let mut lastdump_t = Instant::now();

        // set qdisc rates to 60% of base rate to make sure we start with sane baselines
        self.state_dl.current_rate = self.config.download_base_kbits * 0.6;
        self.state_ul.current_rate = self.config.upload_base_kbits * 0.6;

        Netlink::set_qdisc_rate(
            self.state_dl.qdisc,
            self.state_dl.current_rate.round() as u64,
        )?;
        Netlink::set_qdisc_rate(
            self.state_ul.qdisc,
            self.state_ul.current_rate.round() as u64,
        )?;

        let mut speed_hist_fd: Option<File> = None;
        let mut speed_hist_fd_inner: File;
        let mut stats_fd: Option<File> = None;
        let mut stats_fd_inner: File;

        if !self.config.suppress_statistics {
            speed_hist_fd_inner = File::options()
                .create(true)
                .write(true)
                .truncate(true)
                .open(self.config.speed_hist_file.as_str())?;

            speed_hist_fd_inner.write_all("time,counter,upspeed,downspeed\n".as_bytes())?;
            speed_hist_fd_inner.flush()?;

            speed_hist_fd = Some(speed_hist_fd_inner);

            stats_fd_inner = File::options()
                .create(true)
                .write(true)
                .truncate(true)
                .open(self.config.stats_file.as_str())?;

            stats_fd_inner.write_all(
                "times,timens,rxload,txload,deltadelaydown,deltadelayup,dlrate,uprate\n".as_bytes(),
            )?;
            stats_fd_inner.flush()?;

            stats_fd = Some(stats_fd_inner);
        }

        loop {
            sleep(sleep_time);
            let now_t = Instant::now();

            if now_t.duration_since(lastchg_t).as_secs_f64() > self.config.min_change_interval {
                // if it's been long enough, and the stats indicate needing to change speeds
                // change speeds here

                (self.state_dl.current_bytes, self.state_ul.current_bytes) =
                    get_interface_stats(&self.config, self.down_direction, self.up_direction)?;

                self.update_deltas()?;

                if self.state_dl.deltas.is_empty() || self.state_ul.deltas.is_empty() {
                    warn!("No reflector data available, dropping to minimum rates");
                    self.state_dl.next_rate = self.config.download_min_kbits;
                    self.state_ul.next_rate = self.config.upload_min_kbits;

                    Netlink::set_qdisc_rate(
                        self.state_dl.qdisc,
                        self.state_dl.next_rate as u64,
                    )?;
                    Netlink::set_qdisc_rate(
                        self.state_ul.qdisc,
                        self.state_ul.next_rate as u64,
                    )?;

                    self.state_dl.current_rate = self.state_dl.next_rate;
                    self.state_ul.current_rate = self.state_ul.next_rate;
                    continue;
                }

                self.calculate_rate(Direction::Down)?;
                self.calculate_rate(Direction::Up)?;

                let dl_changed = (self.state_dl.next_rate - self.state_dl.current_rate).abs() > 1.0;
                let ul_changed = (self.state_ul.next_rate - self.state_ul.current_rate).abs() > 1.0;

                if dl_changed || ul_changed {
                    info!(
                        "self.state_ul.next_rate {} self.state_dl.next_rate {}",
                        self.state_ul.next_rate, self.state_dl.next_rate
                    );
                }

                if dl_changed {
                    Netlink::set_qdisc_rate(self.state_dl.qdisc, self.state_dl.next_rate as u64)?;
                }

                if ul_changed {
                    Netlink::set_qdisc_rate(self.state_ul.qdisc, self.state_ul.next_rate as u64)?;
                }

                self.state_dl.current_rate = self.state_dl.next_rate;
                self.state_ul.current_rate = self.state_ul.next_rate;

                let stats_time = Time::new(ClockId::Realtime);
                debug!(
                    "{},{},{},{},{},{},{},{}",
                    stats_time.secs(),
                    stats_time.nsecs(),
                    self.state_dl.load,
                    self.state_ul.load,
                    self.state_dl.delta_stat,
                    self.state_ul.delta_stat,
                    self.state_dl.current_rate as u64,
                    self.state_ul.current_rate as u64
                );

                if let Some(ref mut fd) = stats_fd {
                    if let Err(e) = fd
                        .write_all(
                            format!(
                                "{},{},{},{},{},{},{},{}\n",
                                stats_time.secs(),
                                stats_time.nsecs(),
                                self.state_dl.load,
                                self.state_ul.load,
                                self.state_dl.delta_stat,
                                self.state_ul.delta_stat,
                                self.state_dl.current_rate as u64,
                                self.state_ul.current_rate as u64
                            )
                            .as_bytes(),
                        )
                        .and_then(|_| fd.flush())
                    {
                        warn!("Failed to write statistics: {}", e);
                    }
                }

                lastchg_t = now_t;
            }

            if let Some(ref mut fd) = speed_hist_fd {
                if now_t.duration_since(lastdump_t).as_secs_f64() > 300.0 {
                    // rewind and truncate so the file doesn't grow forever
                    if let Err(e) = fd.rewind().and_then(|_| fd.set_len(0)) {
                        warn!("Failed to truncate speed history file: {}", e);
                    }

                    let _ = fd.write_all("time,counter,upspeed,downspeed\n".as_bytes());

                    let hist_time = Time::new(ClockId::Realtime);
                    for i in 0..self.config.speed_hist_size as usize {
                        if let Err(e) = fd.write_all(
                            format!(
                                "{},{},{},{}\n",
                                hist_time.as_secs_f64(),
                                i,
                                self.state_ul.safe_rates[i],
                                self.state_dl.safe_rates[i]
                            )
                            .as_bytes(),
                        ) {
                            warn!("Failed to write speed history file: {}", e);
                        }
                    }

                    if let Err(e) = fd.flush() {
                        warn!("Failed to flush speed history file: {}", e);
                    }

                    lastdump_t = now_t;
                }
            }
        }
    }
}
