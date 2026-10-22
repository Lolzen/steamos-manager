/*
 * SPDX-License-Identifier: MIT
 */

//! Services of the user's session, independent of the init system.
//!
//! With systemd they are user units, exactly as before. Without a user
//! service manager, as on runit systems (Void Linux), steamos-manager runs
//! them itself as child processes, the way their unit runs them: the same
//! command, with the variables of the same environment file.

use anyhow::Result;
use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use std::collections::{HashMap, VecDeque};
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::time::Duration;
use tokio::process::{Child, Command};
use tokio::select;
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, timeout};
use tokio_util::sync::CancellationToken;
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

// Restart=on-failure with systemd's defaults: RestartSec=100ms, at most
// StartLimitBurst=5 starts within StartLimitIntervalSec=10s
#[cfg(not(test))]
const RESTART_DELAY: Duration = Duration::from_millis(100);
#[cfg(test)]
const RESTART_DELAY: Duration = Duration::from_millis(10);
const START_LIMIT_BURST: usize = 5;
const START_LIMIT_INTERVAL: Duration = Duration::from_secs(10);

struct Running {
    stop: CancellationToken,
    supervisor: JoinHandle<()>,
}

pub(crate) struct ChildService {
    executable: PathBuf,
    environment_file: Option<PathBuf>,
    restart_on_failure: bool,
    running: Option<Running>,
}

// As Restart=on-failure: an unclean exit code or a signal other than the
// clean ones (SIGHUP, SIGINT, SIGTERM, SIGPIPE)
fn failed(status: ExitStatus) -> bool {
    match status.signal() {
        Some(signal) => ![
            Signal::SIGHUP as i32,
            Signal::SIGINT as i32,
            Signal::SIGTERM as i32,
            Signal::SIGPIPE as i32,
        ]
        .contains(&signal),
        None => !status.success(),
    }
}

impl ChildService {
    pub(crate) fn new(executable: impl AsRef<Path>) -> ChildService {
        ChildService {
            executable: executable.as_ref().to_path_buf(),
            environment_file: None,
            restart_on_failure: false,
            running: None,
        }
    }

    /// Like systemd's `EnvironmentFile=-<file>`: read each time the service
    /// starts, skipped if it does not exist
    pub(crate) fn environment_file(mut self, file: impl AsRef<Path>) -> ChildService {
        self.environment_file = Some(file.as_ref().to_path_buf());
        self
    }

    /// Like systemd's `Restart=on-failure`
    pub(crate) fn restart_on_failure(mut self) -> ChildService {
        self.restart_on_failure = true;
        self
    }

    #[cfg(test)]
    fn is_running(&self) -> bool {
        self.running
            .as_ref()
            .is_some_and(|running| !running.supervisor.is_finished())
    }

    async fn spawn(executable: &Path, environment_file: Option<&Path>) -> Result<Child> {
        let environment = match environment_file {
            Some(file) => read_environment_file(file).await?,
            None => HashMap::new(),
        };
        let child = Command::new(path(executable))
            .envs(environment)
            .stdin(Stdio::null())
            .spawn()?;
        debug!("Started {} as {:?}", executable.display(), child.id());
        Ok(child)
    }

    // SIGTERM, and SIGKILL if it does not exit in time, as systemd stops a
    // service; the child is reaped here
    async fn terminate(executable: &Path, child: &mut Child) {
        if let Some(pid) = child.id().and_then(|pid| i32::try_from(pid).ok()) {
            let _ = kill(Pid::from_raw(pid), Signal::SIGTERM);
        }
        if timeout(STOP_TIMEOUT, child.wait()).await.is_err() {
            warn!(
                "{} did not exit after SIGTERM, killing it",
                executable.display()
            );
            let _ = child.kill().await;
        }
    }

    async fn start(&mut self) -> Result<()> {
        // The first start reports its errors to the caller; restarts after a
        // failure happen in the supervising task
        let mut child = Self::spawn(&self.executable, self.environment_file.as_deref()).await?;
        let stop = CancellationToken::new();

        let executable = self.executable.clone();
        let environment_file = self.environment_file.clone();
        let restart_on_failure = self.restart_on_failure;
        let task_stop = stop.clone();
        let supervisor = tokio::spawn(async move {
            let mut starts = VecDeque::from([Instant::now()]);
            loop {
                let status = select! {
                    status = child.wait() => status,
                    () = task_stop.cancelled() => {
                        Self::terminate(&executable, &mut child).await;
                        return;
                    }
                };
                let Ok(status) = status else {
                    return;
                };
                if !restart_on_failure || !failed(status) {
                    return;
                }
                starts.retain(|start| start.elapsed() < START_LIMIT_INTERVAL);
                if starts.len() >= START_LIMIT_BURST {
                    warn!(
                        "{} failed too often, not restarting it",
                        executable.display()
                    );
                    return;
                }
                warn!("{} failed ({status}), restarting it", executable.display());
                select! {
                    () = sleep(RESTART_DELAY) => (),
                    () = task_stop.cancelled() => return,
                }
                child = match Self::spawn(&executable, environment_file.as_deref()).await {
                    Ok(child) => child,
                    Err(e) => {
                        warn!("Could not restart {}: {e}", executable.display());
                        return;
                    }
                };
                starts.push_back(Instant::now());
            }
        });
        self.running = Some(Running { stop, supervisor });
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
        // The supervisor stops restarting, terminates the child and reaps it
        running.stop.cancel();
        (&mut running.supervisor).await?;
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
    use tokio::fs::{create_dir_all, read_to_string, remove_file, set_permissions, write};
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
        wait_for(&out, "bar\n").await;

        // A restart starts it again, with the environment file read again
        remove_file(&out).await.expect("remove_file");
        write(path("/tmp/env"), "FOO=baz\n").await.expect("write");
        service.restart().await.unwrap();
        assert!(service.is_running());
        wait_for(&out, "baz\n").await;

        service.stop().await.unwrap();
        assert!(!service.is_running());
    }

    async fn wait_for(file: &Path, contents: &str) {
        for _ in 0..100 {
            if read_to_string(file).await.is_ok_and(|s| s == contents) {
                return;
            }
            sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(read_to_string(file).await.unwrap(), contents);
    }

    async fn runs(file: &Path) -> usize {
        read_to_string(file)
            .await
            .map(|s| s.lines().count())
            .unwrap_or(0)
    }

    #[tokio::test]
    async fn restart_on_failure() {
        let _h = testing::start();

        let count = path("/tmp/count");
        create_dir_all(count.parent().unwrap())
            .await
            .expect("create_dir_all");
        let script = format!("#!/bin/sh\necho run >> {}\nexit 1\n", count.display());
        let exe = executable("failing", &script).await;

        // Without the option a failure is left as it is
        let mut service = ChildService::new(&exe);
        service.restart().await.unwrap();
        sleep(Duration::from_millis(300)).await;
        assert_eq!(runs(&count).await, 1);
        assert!(!service.is_running());

        // With it, restarted until the start limit
        remove_file(&count).await.expect("remove_file");
        let mut service = ChildService::new(&exe).restart_on_failure();
        service.restart().await.unwrap();
        for _ in 0..100 {
            if !service.is_running() {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
        assert!(!service.is_running());
        assert_eq!(runs(&count).await, START_LIMIT_BURST);
        service.stop().await.unwrap();
    }

    #[tokio::test]
    async fn clean_exit_not_restarted() {
        let _h = testing::start();

        let count = path("/tmp/count");
        create_dir_all(count.parent().unwrap())
            .await
            .expect("create_dir_all");
        let script = format!("#!/bin/sh\necho run >> {}\nexit 0\n", count.display());
        let exe = executable("clean", &script).await;

        let mut service = ChildService::new(&exe).restart_on_failure();
        service.restart().await.unwrap();
        sleep(Duration::from_millis(300)).await;
        assert_eq!(runs(&count).await, 1);
        assert!(!service.is_running());
    }

    #[tokio::test]
    async fn stop_ends_restarts() {
        let _h = testing::start();

        let count = path("/tmp/count");
        create_dir_all(count.parent().unwrap())
            .await
            .expect("create_dir_all");
        let script = format!(
            "#!/bin/sh\necho run >> {}\nsleep 0.05\nexit 1\n",
            count.display()
        );
        let exe = executable("flaky", &script).await;

        let mut service = ChildService::new(&exe).restart_on_failure();
        service.restart().await.unwrap();
        sleep(Duration::from_millis(30)).await;
        service.stop().await.unwrap();
        assert!(!service.is_running());
        let after_stop = runs(&count).await;
        sleep(Duration::from_millis(300)).await;
        assert_eq!(runs(&count).await, after_stop);
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
