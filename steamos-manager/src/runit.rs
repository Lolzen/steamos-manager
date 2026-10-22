/*
 * SPDX-License-Identifier: MIT
 */

//! Services supervised by runit, as used by Void Linux.
//!
//! Units are mapped by name: `foo.service` is the runit service `foo`, whose
//! definition lives in `/etc/sv/foo`. runit supervises a service while it is
//! linked into the service directory (`$SVDIR`, `/var/service` by default);
//! in systemd's terms a linked service without a `down` file is enabled, as
//! it is started at boot. Services are controlled with sv(8) through their
//! definition, which keeps working while runsv(8) still supervises a service
//! that was just unlinked.

use anyhow::{Result, bail};
use std::fs::OpenOptions;
use std::io::ErrorKind;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::fs::{remove_file, symlink, symlink_metadata, try_exists};
use tokio::task::spawn_blocking;
use tokio::time::{Instant, sleep};

use crate::process::run_script;
use crate::systemd::{ActiveState, EnableState, JobResult};
use crate::{path, try_read_to_string};

const SV: &str = "sv";
const SERVICE_DEFINITIONS: &str = "/etc/sv";
const DEFAULT_SERVICE_DIRECTORY: &str = "/var/service";

// sv(8) waits up to 7 seconds by default before reporting a timeout
#[cfg(not(test))]
const JOB_TIMEOUT: Duration = Duration::from_secs(7);
#[cfg(test)]
const JOB_TIMEOUT: Duration = Duration::from_millis(300);
const JOB_POLL_INTERVAL: Duration = Duration::from_millis(100);

fn service_directory() -> PathBuf {
    match std::env::var_os("SVDIR") {
        Some(dir) if !dir.is_empty() => path(PathBuf::from(dir)),
        _ => path(DEFAULT_SERVICE_DIRECTORY),
    }
}

pub struct RunitService {
    name: String,
}

pub struct RunitJob {
    definition: PathBuf,
    target: Option<ActiveState>,
}

impl RunitService {
    pub fn new(unit: &str) -> RunitService {
        RunitService {
            name: String::from(unit.strip_suffix(".service").unwrap_or(unit)),
        }
    }

    fn definition(&self) -> PathBuf {
        path(SERVICE_DEFINITIONS).join(&self.name)
    }

    fn link(&self) -> PathBuf {
        service_directory().join(&self.name)
    }

    pub async fn exists(&self) -> Result<bool> {
        Ok(try_exists(self.definition().join("run")).await?)
    }

    async fn is_linked(&self) -> Result<bool> {
        match symlink_metadata(self.link()).await {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn active(&self) -> Result<ActiveState> {
        active_state(&self.definition()).await
    }

    pub async fn enabled(&self) -> Result<EnableState> {
        if self.is_linked().await? && !try_exists(self.definition().join("down")).await? {
            Ok(EnableState::Enabled)
        } else {
            Ok(EnableState::Disabled)
        }
    }

    async fn sv(&self, command: &str, target: Option<ActiveState>) -> Result<RunitJob> {
        let definition = self.definition();
        let service = definition.to_string_lossy().into_owned();
        run_script(SV, &[command, service.as_str()]).await?;
        Ok(RunitJob { definition, target })
    }

    async fn sv_supervised(&self, command: &str, target: Option<ActiveState>) -> Result<RunitJob> {
        if !is_supervised(&self.definition()).await? {
            bail!(
                "Service {} is not supervised by runit (not linked in {})",
                self.name,
                service_directory().display()
            );
        }
        self.sv(command, target).await
    }

    pub async fn start(&self) -> Result<RunitJob> {
        self.sv_supervised("up", Some(ActiveState::Active)).await
    }

    pub async fn stop(&self) -> Result<RunitJob> {
        // Nothing runs a service that runit does not supervise
        if !is_supervised(&self.definition()).await? {
            return Ok(RunitJob {
                definition: self.definition(),
                target: None,
            });
        }
        self.sv("down", Some(ActiveState::Inactive)).await
    }

    #[allow(unused)]
    pub async fn reload(&self) -> Result<RunitJob> {
        self.sv_supervised("hup", None).await
    }

    #[allow(unused)]
    pub async fn enable(&self) -> Result<bool> {
        if self.is_linked().await? {
            return Ok(false);
        }
        symlink(self.definition(), self.link()).await?;
        Ok(true)
    }

    #[allow(unused)]
    pub async fn disable(&self) -> Result<bool> {
        match remove_file(self.link()).await {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e.into()),
        }
    }
}

impl RunitJob {
    pub async fn wait(self) -> Result<JobResult> {
        let Some(target) = self.target else {
            return Ok(JobResult::Done);
        };
        let deadline = Instant::now() + JOB_TIMEOUT;
        loop {
            if active_state(&self.definition).await? == target {
                return Ok(JobResult::Done);
            }
            if Instant::now() >= deadline {
                return Ok(JobResult::Timeout);
            }
            sleep(JOB_POLL_INTERVAL).await;
        }
    }
}

async fn is_supervised(definition: &Path) -> Result<bool> {
    // As sv(8) does: runsv(8) holds supervise/ok open for reading while it
    // runs, so opening it for writing without blocking fails with ENXIO
    // when no runsv is there
    let ok = definition.join("supervise/ok");
    spawn_blocking(move || {
        match OpenOptions::new()
            .write(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(ok)
        {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(false),
            Err(e) if e.raw_os_error() == Some(libc::ENXIO) => Ok(false),
            Err(e) => Err(e.into()),
        }
    })
    .await?
}

async fn active_state(definition: &Path) -> Result<ActiveState> {
    if !is_supervised(definition).await? {
        return Ok(ActiveState::Inactive);
    }
    // runsv(8) writes the current state to supervise/stat: "run", "down"
    // or "finish", optionally followed by flags such as ", want down"
    let Some(stat) = try_read_to_string(definition.join("supervise/stat")).await? else {
        return Ok(ActiveState::Inactive);
    };
    let state = stat.split(',').next().unwrap_or_default().trim();
    Ok(match state {
        "run" => ActiveState::Active,
        "finish" => ActiveState::Deactivating,
        _ => ActiveState::Inactive,
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::process::test::{code, ok};
    use crate::testing;
    use nix::sys::stat::Mode;
    use nix::unistd::mkfifo;
    use std::fs::File;
    use tokio::fs::{create_dir_all, write};

    async fn define(name: &str) {
        let definition = path(SERVICE_DEFINITIONS).join(name);
        create_dir_all(&definition).await.expect("create_dir_all");
        write(definition.join("run"), "#!/bin/sh\n")
            .await
            .expect("write");
    }

    async fn link(name: &str) {
        create_dir_all(path(DEFAULT_SERVICE_DIRECTORY))
            .await
            .expect("create_dir_all");
        symlink(
            path(SERVICE_DEFINITIONS).join(name),
            path(DEFAULT_SERVICE_DIRECTORY).join(name),
        )
        .await
        .expect("symlink");
    }

    // Stands in for runsv(8): the supervise directory with the ok FIFO held
    // open for reading, as long as the returned file lives
    async fn supervise(name: &str, stat: &str) -> File {
        let supervise = path(SERVICE_DEFINITIONS).join(name).join("supervise");
        create_dir_all(&supervise).await.expect("create_dir_all");
        mkfifo(&supervise.join("ok"), Mode::S_IRUSR | Mode::S_IWUSR).expect("mkfifo");
        set_stat(name, stat).await;
        OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(supervise.join("ok"))
            .expect("open")
    }

    async fn set_stat(name: &str, stat: &str) {
        let supervise = path(SERVICE_DEFINITIONS).join(name).join("supervise");
        write(supervise.join("stat"), stat).await.expect("write");
    }

    #[test]
    fn unit_name() {
        assert_eq!(RunitService::new("foo.service").name, "foo");
        assert_eq!(RunitService::new("foo").name, "foo");
        assert_eq!(
            RunitService::new("jupiter-fan-control.service").name,
            "jupiter-fan-control"
        );
    }

    #[tokio::test]
    async fn exists() {
        let _h = testing::start();

        let service = RunitService::new("foo.service");
        assert!(!service.exists().await.unwrap());

        define("foo").await;
        assert!(service.exists().await.unwrap());
    }

    #[tokio::test]
    async fn active() {
        let _h = testing::start();

        define("foo").await;
        let service = RunitService::new("foo.service");
        assert_eq!(service.active().await.unwrap(), ActiveState::Inactive);

        let runsv = supervise("foo", "run\n").await;
        assert_eq!(service.active().await.unwrap(), ActiveState::Active);

        set_stat("foo", "down\n").await;
        assert_eq!(service.active().await.unwrap(), ActiveState::Inactive);

        set_stat("foo", "finish, want down\n").await;
        assert_eq!(service.active().await.unwrap(), ActiveState::Deactivating);

        set_stat("foo", "run, want down\n").await;
        assert_eq!(service.active().await.unwrap(), ActiveState::Active);

        // A stale stat file without runsv does not count
        drop(runsv);
        assert_eq!(service.active().await.unwrap(), ActiveState::Inactive);
    }

    #[tokio::test]
    async fn enabled() {
        let _h = testing::start();

        define("foo").await;
        let service = RunitService::new("foo.service");
        assert_eq!(service.enabled().await.unwrap(), EnableState::Disabled);

        link("foo").await;
        assert_eq!(service.enabled().await.unwrap(), EnableState::Enabled);

        // A down file keeps a linked service from starting at boot
        write(path(SERVICE_DEFINITIONS).join("foo/down"), "")
            .await
            .expect("write");
        assert_eq!(service.enabled().await.unwrap(), EnableState::Disabled);
    }

    #[tokio::test]
    async fn enable_disable() {
        let _h = testing::start();

        define("foo").await;
        create_dir_all(path(DEFAULT_SERVICE_DIRECTORY))
            .await
            .expect("create_dir_all");
        let service = RunitService::new("foo.service");

        assert!(service.enable().await.unwrap());
        assert!(!service.enable().await.unwrap());
        assert_eq!(service.enabled().await.unwrap(), EnableState::Enabled);

        assert!(service.disable().await.unwrap());
        assert!(!service.disable().await.unwrap());
        assert_eq!(service.enabled().await.unwrap(), EnableState::Disabled);
    }

    #[tokio::test]
    async fn not_supervised() {
        let h = testing::start();

        define("foo").await;
        h.test.set_process_cb(ok).await;
        let service = RunitService::new("foo.service");

        assert!(service.start().await.is_err());
        assert!(service.reload().await.is_err());
        // Stopping what does not run is not an error
        let job = service.stop().await.unwrap();
        assert_eq!(job.wait().await.unwrap(), JobResult::Done);
    }

    #[tokio::test]
    async fn start_stop() {
        let h = testing::start();

        define("foo").await;
        link("foo").await;
        let _runsv = supervise("foo", "down\n").await;
        h.test.set_process_cb(ok).await;
        let service = RunitService::new("foo.service");

        let job = service.start().await.unwrap();
        set_stat("foo", "run\n").await;
        assert_eq!(job.wait().await.unwrap(), JobResult::Done);

        let job = service.stop().await.unwrap();
        set_stat("foo", "down\n").await;
        assert_eq!(job.wait().await.unwrap(), JobResult::Done);

        let job = service.reload().await.unwrap();
        assert_eq!(job.wait().await.unwrap(), JobResult::Done);

        h.test.set_process_cb(code).await;
        assert!(service.start().await.is_err());
    }

    #[tokio::test]
    async fn wait_timeout() {
        let h = testing::start();

        define("foo").await;
        link("foo").await;
        let _runsv = supervise("foo", "down\n").await;
        h.test.set_process_cb(ok).await;

        let service = RunitService::new("foo.service");
        let job = service.start().await.unwrap();
        assert_eq!(job.wait().await.unwrap(), JobResult::Timeout);
    }
}
