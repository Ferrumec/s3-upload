//! Per-user concurrency and byte quotas.
//!
//! All state is in memory: it resets when the process restarts and is not shared
//! between instances. (The persistent per-user storage quota lives in the database,
//! see `model::storage_used`.)

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    time::Instant,
};

use uuid::Uuid;

use crate::config::Config;

struct UserEntry {
    window_start: Instant,
    upload_bytes: u64,
    download_bytes: u64,
    active_uploads: u32,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Denied {
    /// Too many simultaneous uploads from this user.
    Busy,
    /// Byte quota for the current window is used up; retry after N seconds.
    Quota { retry_after: u64 },
}

pub struct UserLimits {
    inner: Mutex<HashMap<Uuid, UserEntry>>,
    window: std::time::Duration,
    max_active_uploads: u32,
    upload_quota: u64,
    download_quota: u64,
}

impl UserLimits {
    pub fn new(cfg: &Config) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(HashMap::new()),
            window: cfg.quota_window,
            max_active_uploads: cfg.max_uploads_per_user,
            upload_quota: cfg.user_upload_quota,
            download_quota: cfg.user_download_quota,
        })
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<Uuid, UserEntry>> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Fetch (creating if needed) the entry for `user`, rolling its window if expired.
    /// Also returns the seconds left in the window.
    fn entry<'a>(
        &self,
        map: &'a mut HashMap<Uuid, UserEntry>,
        user: Uuid,
    ) -> (&'a mut UserEntry, u64) {
        let now = Instant::now();
        let e = map.entry(user).or_insert(UserEntry {
            window_start: now,
            upload_bytes: 0,
            download_bytes: 0,
            active_uploads: 0,
        });
        if now.duration_since(e.window_start) >= self.window {
            e.window_start = now;
            e.upload_bytes = 0;
            e.download_bytes = 0;
        }
        let left = self
            .window
            .saturating_sub(now.duration_since(e.window_start))
            .as_secs()
            .max(1);
        (e, left)
    }

    /// Reserve an upload slot. `declared_len` is the Content-Length (0 if unknown):
    /// a request that cannot possibly fit in the remaining quota is refused early.
    pub fn begin_upload(
        self: &Arc<Self>,
        user: Uuid,
        declared_len: u64,
    ) -> Result<UploadSlot, Denied> {
        let mut map = self.lock();
        let (e, left) = self.entry(&mut map, user);
        if e.active_uploads >= self.max_active_uploads {
            return Err(Denied::Busy);
        }
        if e.upload_bytes >= self.upload_quota
            || e.upload_bytes.saturating_add(declared_len) > self.upload_quota
        {
            return Err(Denied::Quota { retry_after: left });
        }
        e.active_uploads += 1;
        Ok(UploadSlot {
            limits: Arc::clone(self),
            user,
        })
    }

    /// Count `n` more uploaded bytes against the user's quota.
    pub fn charge_upload(&self, user: Uuid, n: u64) -> Result<(), u64> {
        let mut map = self.lock();
        let (e, left) = self.entry(&mut map, user);
        if e.upload_bytes.saturating_add(n) > self.upload_quota {
            return Err(left);
        }
        e.upload_bytes += n;
        Ok(())
    }

    /// Check (without charging) that a download of `len` bytes fits the quota.
    pub fn download_allowed(&self, user: Uuid, len: u64) -> Result<(), u64> {
        let mut map = self.lock();
        let (e, left) = self.entry(&mut map, user);
        if e.download_bytes.saturating_add(len) > self.download_quota {
            return Err(left);
        }
        Ok(())
    }

    /// Count `n` more downloaded bytes against the user's quota.
    pub fn charge_download(&self, user: Uuid, n: u64) -> Result<(), u64> {
        let mut map = self.lock();
        let (e, left) = self.entry(&mut map, user);
        if e.download_bytes.saturating_add(n) > self.download_quota {
            return Err(left);
        }
        e.download_bytes += n;
        Ok(())
    }

    /// Drop idle entries whose window has expired so the map cannot grow forever.
    pub fn sweep(&self) {
        let now = Instant::now();
        let window = self.window;
        self.lock()
            .retain(|_, e| e.active_uploads > 0 || now.duration_since(e.window_start) < window);
    }
}

/// Releases the user's upload slot when dropped (success, error or disconnect).
pub struct UploadSlot {
    limits: Arc<UserLimits>,
    user: Uuid,
}

impl Drop for UploadSlot {
    fn drop(&mut self) {
        if let Some(e) = self.limits.lock().get_mut(&self.user) {
            e.active_uploads = e.active_uploads.saturating_sub(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn user(n: u128) -> Uuid {
        Uuid::from_u128(n)
    }

    fn limits(max_active: u32, up: u64, down: u64, window_ms: u64) -> Arc<UserLimits> {
        Arc::new(UserLimits {
            inner: Mutex::new(HashMap::new()),
            window: Duration::from_millis(window_ms),
            max_active_uploads: max_active,
            upload_quota: up,
            download_quota: down,
        })
    }

    #[test]
    fn upload_slots_are_limited_per_user_and_released_on_drop() {
        let c = limits(2, 1000, 1000, 60_000);
        let a = c.begin_upload(user(1), 0).unwrap();
        let _b = c.begin_upload(user(1), 0).unwrap();
        assert_eq!(c.begin_upload(user(1), 0).err(), Some(Denied::Busy));
        assert!(c.begin_upload(user(2), 0).is_ok()); // other users unaffected
        drop(a);
        assert!(c.begin_upload(user(1), 0).is_ok());
    }

    #[test]
    fn declared_length_beyond_remaining_quota_is_refused_early() {
        let c = limits(5, 100, 100, 60_000);
        c.charge_upload(user(1), 60).unwrap();
        assert!(matches!(
            c.begin_upload(user(1), 50).err(),
            Some(Denied::Quota { .. })
        ));
        assert!(c.begin_upload(user(1), 40).is_ok());
    }

    #[test]
    fn upload_quota_is_enforced_and_resets_after_the_window() {
        let c = limits(5, 100, 100, 50);
        assert!(c.charge_upload(user(1), 100).is_ok());
        assert!(c.charge_upload(user(1), 1).is_err());
        std::thread::sleep(Duration::from_millis(80));
        assert!(c.charge_upload(user(1), 100).is_ok());
    }

    #[test]
    fn download_quota_is_checked_and_charged() {
        let c = limits(5, 100, 100, 60_000);
        assert!(c.download_allowed(user(1), 100).is_ok());
        assert!(c.download_allowed(user(1), 101).is_err());
        c.charge_download(user(1), 70).unwrap();
        assert!(c.charge_download(user(1), 31).is_err());
        assert!(c.download_allowed(user(1), 30).is_ok());
    }

    #[test]
    fn sweep_removes_expired_idle_users_but_keeps_active_ones() {
        let c = limits(5, 100, 100, 30);
        c.charge_upload(user(9), 1).unwrap();
        let _active = c.begin_upload(user(1), 0).unwrap();
        std::thread::sleep(Duration::from_millis(60));
        c.sweep();
        let map = c.lock();
        assert!(!map.contains_key(&user(9)));
        assert!(map.contains_key(&user(1)));
    }
}
