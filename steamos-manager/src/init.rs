/*
 * SPDX-License-Identifier: MIT
 */

//! System services, independent of the init system.
//!
//! systemd is used whenever the system was booted with it, exactly as before.
//! Systems booted with runit (Void Linux) use runit's services instead, found
//! by the unit's name (see [`crate::runit`]). Any other system keeps using
//! systemd, so nothing changes where neither is detected.

use anyhow::Result;
use tokio::fs::try_exists;
use zbus::Connection;

use crate::runit::{RunitJob, RunitService};
use crate::systemd::{ActiveState, EnableState, JobMode, JobResult, SystemdJobWaiter, SystemdUnit};
use crate::{path, try_read_to_string};

#[derive(PartialEq, Debug, Copy, Clone)]
pub(crate) enum InitSystem {
    Systemd,
    Runit,
}

pub(crate) async fn init_system() -> InitSystem {
    // The same check as sd_booted(3)
    if try_exists(path("/run/systemd/system"))
        .await
        .unwrap_or(false)
    {
        return InitSystem::Systemd;
    }
    match try_read_to_string(path("/proc/1/comm")).await {
        Ok(Some(comm)) if comm.trim() == "runit" => InitSystem::Runit,
        _ => InitSystem::Systemd,
    }
}

pub(crate) enum ServiceUnit<'dbus> {
    Systemd(SystemdUnit<'dbus>),
    Runit(RunitService),
}

pub(crate) enum ServiceJob {
    Systemd(Box<SystemdJobWaiter>),
    Runit(RunitJob),
}

impl<'dbus> ServiceUnit<'dbus> {
    pub(crate) async fn new(connection: &Connection, name: &str) -> Result<ServiceUnit<'dbus>> {
        Ok(match init_system().await {
            InitSystem::Systemd => ServiceUnit::Systemd(SystemdUnit::new(connection, name).await?),
            InitSystem::Runit => ServiceUnit::Runit(RunitService::new(name)),
        })
    }

    pub(crate) async fn exists(connection: &Connection, name: &str) -> Result<bool> {
        match init_system().await {
            InitSystem::Systemd => SystemdUnit::exists(connection, name).await,
            InitSystem::Runit => RunitService::new(name).exists().await,
        }
    }

    pub(crate) async fn active(&self) -> Result<ActiveState> {
        match self {
            ServiceUnit::Systemd(unit) => unit.active().await,
            ServiceUnit::Runit(service) => service.active().await,
        }
    }

    pub(crate) async fn enabled(&self) -> Result<EnableState> {
        match self {
            ServiceUnit::Systemd(unit) => unit.enabled().await,
            ServiceUnit::Runit(service) => service.enabled().await,
        }
    }

    pub(crate) async fn start(&self, job_mode: JobMode) -> Result<ServiceJob> {
        Ok(match self {
            ServiceUnit::Systemd(unit) => {
                ServiceJob::Systemd(Box::new(unit.start(job_mode).await?))
            }
            ServiceUnit::Runit(service) => ServiceJob::Runit(service.start().await?),
        })
    }

    pub(crate) async fn stop(&self, job_mode: JobMode) -> Result<ServiceJob> {
        Ok(match self {
            ServiceUnit::Systemd(unit) => ServiceJob::Systemd(Box::new(unit.stop(job_mode).await?)),
            ServiceUnit::Runit(service) => ServiceJob::Runit(service.stop().await?),
        })
    }

    pub(crate) async fn reload(&self, job_mode: JobMode) -> Result<ServiceJob> {
        Ok(match self {
            ServiceUnit::Systemd(unit) => {
                ServiceJob::Systemd(Box::new(unit.reload(job_mode).await?))
            }
            ServiceUnit::Runit(service) => ServiceJob::Runit(service.reload().await?),
        })
    }

    pub(crate) async fn enable(&self) -> Result<bool> {
        match self {
            ServiceUnit::Systemd(unit) => unit.enable().await,
            ServiceUnit::Runit(service) => service.enable().await,
        }
    }

    pub(crate) async fn disable(&self) -> Result<bool> {
        match self {
            ServiceUnit::Systemd(unit) => unit.disable().await,
            ServiceUnit::Runit(service) => service.disable().await,
        }
    }
}

impl ServiceJob {
    pub(crate) async fn wait(self) -> Result<JobResult> {
        match self {
            ServiceJob::Systemd(waiter) => waiter.wait().await,
            ServiceJob::Runit(job) => job.wait().await,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::testing;
    use tokio::fs::{create_dir_all, write};

    #[tokio::test]
    async fn detect_systemd() {
        let _h = testing::start();

        create_dir_all(path("/run/systemd/system"))
            .await
            .expect("create_dir_all");
        assert_eq!(init_system().await, InitSystem::Systemd);

        // A booted systemd wins even if PID 1 were called runit
        create_dir_all(path("/proc/1"))
            .await
            .expect("create_dir_all");
        write(path("/proc/1/comm"), "runit\n").await.expect("write");
        assert_eq!(init_system().await, InitSystem::Systemd);
    }

    #[tokio::test]
    async fn detect_runit() {
        let _h = testing::start();

        create_dir_all(path("/proc/1"))
            .await
            .expect("create_dir_all");
        write(path("/proc/1/comm"), "runit\n").await.expect("write");
        assert_eq!(init_system().await, InitSystem::Runit);
    }

    #[tokio::test]
    async fn detect_fallback() {
        let _h = testing::start();

        // Neither detected: keep using systemd, as before
        assert_eq!(init_system().await, InitSystem::Systemd);

        create_dir_all(path("/proc/1"))
            .await
            .expect("create_dir_all");
        write(path("/proc/1/comm"), "init\n").await.expect("write");
        assert_eq!(init_system().await, InitSystem::Systemd);
    }

    #[tokio::test]
    async fn runit_service() {
        let mut h = testing::start();
        let connection = h.new_dbus().await.expect("dbus");

        create_dir_all(path("/proc/1"))
            .await
            .expect("create_dir_all");
        write(path("/proc/1/comm"), "runit\n").await.expect("write");
        create_dir_all(path("/etc/sv/foo"))
            .await
            .expect("create_dir_all");
        write(path("/etc/sv/foo/run"), "#!/bin/sh\n")
            .await
            .expect("write");

        assert!(
            ServiceUnit::exists(&connection, "foo.service")
                .await
                .unwrap()
        );
        assert!(
            !ServiceUnit::exists(&connection, "bar.service")
                .await
                .unwrap()
        );
        let service = ServiceUnit::new(&connection, "foo.service").await.unwrap();
        assert!(matches!(service, ServiceUnit::Runit(_)));
        assert_eq!(service.active().await.unwrap(), ActiveState::Inactive);
        assert_eq!(service.enabled().await.unwrap(), EnableState::Disabled);
    }
}
