/*
 * SPDX-License-Identifier: MIT
 */

//! Services of the user's session, independent of the init system.
//!
//! With systemd they are user units, exactly as before. Without a user
//! service manager, as on runit systems (Void Linux), steamos-manager runs
//! them itself as child processes, the way their unit runs them: the same
//! command, with the variables of the same environment file.

use anyhow::{Result, anyhow};
use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::process::Command;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing::{debug, warn};
use zbus::Connection;

use crate::init::{InitSystem, init_system};
use crate::systemd::{JobMode, SystemdUnit};
use crate::{path, try_read_to_string};

// How long a child may take to exit after SIGTERM before it gets SIGKILL
#[cfg(not(test))]
const STOP_TIMEOUT: Duration = Duration::from_secs(10);
#[cfg(test)]
const STOP_TIMEOUT: Duration = Duration::from_millis(500);

pub(crate) enum UserService<'dbus> {
    Systemd(SystemdUnit<'dbus>),
    Child(ChildService),
}

impl<'dbus> UserService<'dbus> {
    /// The user unit `unit` under systemd, otherwise `child`
    pub(crate) async fn new(
        connection: &Connection,
        unit: &str,
        child: ChildService,
    ) -> Result<UserService<'dbus>> {
        Ok(match init_system().await {
            InitSystem::Systemd => UserService::Systemd(SystemdUnit::new(connection, unit).await?),
            InitSystem::Runit => UserService::Child(child),
        })
    }

    pub(crate) async fn restart(&mut self) -> Result<()> {
        match self {
            UserService::Systemd(unit) => {
                unit.restart(JobMode::Fail).await?;
                Ok(())
            }
            UserService::Child(child) => child.restart().await,
        }
    }

    pub(crate) async fn stop(&mut self) -> Result<()> {
        match self {
            UserService::Systemd(unit) => {
                unit.stop(JobMode::Fail).await?;
                Ok(())
            }
            UserService::Child(child) => child.stop().await,
        }
    }
}

struct Running {
    pid: Pid,
    exited: JoinHandle<()>,
}

pub(crate) struct ChildService {
    executable: PathBuf,
    environment_file: Option<PathBuf>,
    running: Option<Running>,
}

impl ChildService {
    pub(crate) fn new(executable: impl AsRef<Path>) -> ChildService {
        ChildService {
            executable: executable.as_ref().to_path_buf(),
            environment_file: None,
            running: None,
        }
    }

    /// Like systemd's `EnvironmentFile=-<file>`: read when the service
    /// starts, skipped if it does not exist
    pub(crate) fn environment_file(mut self, file: impl AsRef<Path>) -> ChildService {
        self.environment_file = Some(file.as_ref().to_path_buf());
        self
    }

    #[cfg(test)]
    fn is_running(&self) -> bool {
        self.running
            .as_ref()
            .is_some_and(|running| !running.exited.is_finished())
    }

    async fn start(&mut self) -> Result<()> {
        let environment = match &self.environment_file {
            Some(file) => read_environment_file(file).await?,
            None => HashMap::new(),
        };
        let mut child = Command::new(path(&self.executable))
            .envs(environment)
            .stdin(Stdio::null())
            .spawn()?;
        let pid = child
            .id()
            .ok_or(anyhow!("{} exited at once", self.executable.display()))?;
        debug!("Started {} as {pid}", self.executable.display());
        let exited = tokio::spawn(async move {
            let _ = child.wait().await;
        });
        self.running = Some(Running {
            pid: Pid::from_raw(i32::try_from(pid)?),
            exited,
        });
        Ok(())
    }

    pub(crate) async fn restart(&mut self) -> Result<()> {
        self.stop().await?;
        self.start().await
    }

    pub(crate) async fn stop(&mut self) -> Result<()> {
        let Some(mut running) = self.running.take() else {
            return Ok(());
        };
        if running.exited.is_finished() {
            return Ok(());
        }
        kill(running.pid, Signal::SIGTERM)?;
        if timeout(STOP_TIMEOUT, &mut running.exited).await.is_err() {
            warn!(
                "{} did not exit after SIGTERM, killing it",
                self.executable.display()
            );
            kill(running.pid, Signal::SIGKILL)?;
            running.exited.await?;
        }
        Ok(())
    }
}

// The subset of systemd.exec(5)'s environment files that such files use:
// KEY=VALUE lines, comments starting with # or ;, values optionally quoted
async fn read_environment_file(file: &Path) -> Result<HashMap<String, String>> {
    let Some(contents) = try_read_to_string(path(file)).await? else {
        return Ok(HashMap::new());
    };
    Ok(contents
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#') && !line.starts_with(';'))
        .filter_map(|line| line.split_once('='))
        .map(|(key, value)| {
            let value = value.trim();
            let value = ['"', '\'']
                .iter()
                .find_map(|q| value.strip_prefix(*q).and_then(|v| v.strip_suffix(*q)))
                .unwrap_or(value);
            (key.trim().to_string(), value.to_string())
        })
        .collect())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::testing;
    use std::fs::Permissions;
    use std::os::unix::fs::PermissionsExt;
    use tokio::fs::{create_dir_all, read_to_string, set_permissions, write};
    use tokio::time::sleep;

    async fn executable(name: &str, script: &str) -> PathBuf {
        let file = path("/usr/bin").join(name);
        create_dir_all(file.parent().unwrap())
            .await
            .expect("create_dir_all");
        write(&file, script).await.expect("write");
        set_permissions(&file, Permissions::from_mode(0o755))
            .await
            .expect("set_permissions");
        PathBuf::from("/usr/bin").join(name)
    }

    #[tokio::test]
    async fn environment_file() {
        let _h = testing::start();

        assert!(
            read_environment_file(Path::new("/run/missing"))
                .await
                .unwrap()
                .is_empty()
        );

        create_dir_all(path("/run")).await.expect("create_dir_all");
        write(
            path("/run/env"),
            "# comment\n; comment\n\nA=1\n B = two \nC=\"three\"\nD='four'\nE=a=b\nnothing\n",
        )
        .await
        .expect("write");
        let env = read_environment_file(Path::new("/run/env")).await.unwrap();
        assert_eq!(env.len(), 5);
        assert_eq!(env["A"], "1");
        assert_eq!(env["B"], "two");
        assert_eq!(env["C"], "three");
        assert_eq!(env["D"], "four");
        assert_eq!(env["E"], "a=b");
    }

    #[tokio::test]
    async fn restart_stop() {
        let _h = testing::start();

        let out = path("/tmp/out");
        create_dir_all(out.parent().unwrap())
            .await
            .expect("create_dir_all");
        let script = format!(
            "#!/bin/sh\necho \"$FOO\" > {}\nexec sleep 30\n",
            out.display()
        );
        let exe = executable("fake-service", &script).await;
        write(path("/tmp/env"), "FOO=bar\n").await.expect("write");

        let mut service = ChildService::new(&exe).environment_file("/tmp/env");
        assert!(!service.is_running());
        // Stopping what does not run is not an error
        service.stop().await.unwrap();

        service.restart().await.unwrap();
        assert!(service.is_running());
        let first = service.running.as_ref().unwrap().pid;
        for _ in 0..50 {
            if read_to_string(&out).await.is_ok_and(|s| s == "bar\n") {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(read_to_string(&out).await.unwrap(), "bar\n");

        service.restart().await.unwrap();
        assert!(service.is_running());
        assert_ne!(service.running.as_ref().unwrap().pid, first);

        service.stop().await.unwrap();
        assert!(!service.is_running());
    }

    #[tokio::test]
    async fn stop_kills() {
        let _h = testing::start();

        let exe = executable(
            "stubborn",
            "#!/bin/sh\ntrap '' TERM\nwhile :; do sleep 1; done\n",
        )
        .await;
        let mut service = ChildService::new(&exe);
        service.restart().await.unwrap();
        // Give the shell time to install its trap
        sleep(Duration::from_millis(100)).await;
        service.stop().await.unwrap();
        assert!(!service.is_running());
    }

    #[tokio::test]
    async fn user_service_runit() {
        let mut h = testing::start();
        let connection = h.new_dbus().await.expect("dbus");

        create_dir_all(path("/proc/1"))
            .await
            .expect("create_dir_all");
        write(path("/proc/1/comm"), "runit\n").await.expect("write");
        let exe = executable("fake-service", "#!/bin/sh\nexec sleep 30\n").await;

        let mut service = UserService::new(&connection, "fake.service", ChildService::new(&exe))
            .await
            .unwrap();
        assert!(matches!(service, UserService::Child(_)));
        service.restart().await.unwrap();
        service.stop().await.unwrap();
    }
}
