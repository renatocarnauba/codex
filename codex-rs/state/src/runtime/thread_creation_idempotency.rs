use super::StateRuntime;
use chrono::Utc;
use codex_protocol::ThreadId;
use sqlx::Row;
use std::path::PathBuf;

impl StateRuntime {
    /// Insert a canonical legacy binding during the leased metadata backfill.
    /// A duplicate `(originator, key)` or `thread_id` is accepted only when the
    /// entire binding is identical; all other collisions fail closed.
    pub async fn backfill_thread_creation_idempotency(
        &self,
        originator: &str,
        key: &str,
        operation_kind: &str,
        thread_id: ThreadId,
    ) -> anyhow::Result<()> {
        self.insert_thread_creation_idempotency(
            originator,
            key,
            operation_kind,
            thread_id,
            "committed",
        )
        .await
    }

    /// Reserve a key before its canonical SessionMeta is flushed.
    pub async fn reserve_thread_creation_idempotency(
        &self,
        originator: &str,
        key: &str,
        operation_kind: &str,
        thread_id: ThreadId,
    ) -> anyhow::Result<()> {
        self.insert_thread_creation_idempotency(
            originator,
            key,
            operation_kind,
            thread_id,
            "pending",
        )
        .await
    }

    async fn insert_thread_creation_idempotency(
        &self,
        originator: &str,
        key: &str,
        operation_kind: &str,
        thread_id: ThreadId,
        status: &str,
    ) -> anyhow::Result<()> {
        sqlx::query(
            r#"
INSERT INTO thread_creation_idempotency (
    originator, idempotency_key, operation_kind, thread_id, status, updated_at
) VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(originator, idempotency_key) DO NOTHING
            "#,
        )
        .bind(originator)
        .bind(key)
        .bind(operation_kind)
        .bind(thread_id.to_string())
        .bind(status)
        .bind(Utc::now().timestamp())
        .execute(self.pool.as_ref())
        .await?;

        let binding = self
            .get_thread_creation_idempotency(originator, key)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("thread creation idempotency reservation disappeared after insert")
            })?;
        let (bound_kind, bound_thread_id, _, bound_committed) = binding;
        if bound_thread_id != thread_id || bound_kind != operation_kind {
            anyhow::bail!(
                "thread creation idempotency key is already bound to {} ({})",
                bound_thread_id,
                bound_kind
            );
        }
        if status == "committed" && !bound_committed {
            sqlx::query(
                r#"
UPDATE thread_creation_idempotency
SET status = 'committed', updated_at = ?
WHERE originator = ? AND idempotency_key = ? AND thread_id = ?
                "#,
            )
            .bind(Utc::now().timestamp())
            .bind(originator)
            .bind(key)
            .bind(thread_id.to_string())
            .execute(self.pool.as_ref())
            .await?;
        }
        Ok(())
    }

    /// Lookup is always indexed by the contract key. The operation kind is
    /// returned for payload validation; it is intentionally not part of the key.
    pub async fn get_thread_creation_idempotency(
        &self,
        originator: &str,
        key: &str,
    ) -> anyhow::Result<Option<(String, ThreadId, Option<PathBuf>, bool)>> {
        let Some(row) = sqlx::query(
            r#"
SELECT i.operation_kind, i.thread_id, i.status, t.rollout_path
FROM thread_creation_idempotency AS i
LEFT JOIN threads AS t ON t.id = i.thread_id
WHERE i.originator = ? AND i.idempotency_key = ?
            "#,
        )
        .bind(originator)
        .bind(key)
        .fetch_optional(self.pool.as_ref())
        .await?
        else {
            return Ok(None);
        };
        Ok(Some((
            row.try_get("operation_kind")?,
            ThreadId::from_string(row.try_get::<String, _>("thread_id")?.as_str())?,
            row.try_get::<Option<String>, _>("rollout_path")?
                .map(PathBuf::from),
            row.try_get::<String, _>("status")? == "committed",
        )))
    }

    /// Commit only the reservation owned by this newly allocated thread.
    pub async fn commit_thread_creation_idempotency(
        &self,
        originator: &str,
        key: &str,
        thread_id: ThreadId,
    ) -> anyhow::Result<bool> {
        let result = sqlx::query(
            r#"
UPDATE thread_creation_idempotency
SET status = 'committed', updated_at = ?
WHERE originator = ? AND idempotency_key = ? AND thread_id = ? AND status = 'pending'
            "#,
        )
        .bind(Utc::now().timestamp())
        .bind(originator)
        .bind(key)
        .bind(thread_id.to_string())
        .execute(self.pool.as_ref())
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// Remove only an uncommitted reservation owned by this thread.
    pub async fn delete_thread_creation_idempotency_reservation(
        &self,
        originator: &str,
        key: &str,
        thread_id: ThreadId,
    ) -> anyhow::Result<bool> {
        let result = sqlx::query(
            r#"
DELETE FROM thread_creation_idempotency
WHERE originator = ? AND idempotency_key = ? AND thread_id = ? AND status = 'pending'
            "#,
        )
        .bind(originator)
        .bind(key)
        .bind(thread_id.to_string())
        .execute(self.pool.as_ref())
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// Recover reservations at process startup. A pending binding becomes
    /// committed once the canonical rollout has populated `threads`; a missing
    /// binding is reclaimed only after its lease expires.
    pub async fn reconcile_thread_creation_idempotency_reservations(
        &self,
        lease_seconds: i64,
    ) -> anyhow::Result<u64> {
        let now = Utc::now().timestamp();
        sqlx::query(
            r#"
UPDATE thread_creation_idempotency
SET status = 'committed', updated_at = ?
WHERE status = 'pending'
  AND EXISTS (SELECT 1 FROM threads WHERE threads.id = thread_creation_idempotency.thread_id)
            "#,
        )
        .bind(now)
        .execute(self.pool.as_ref())
        .await?;
        sqlx::query(
            r#"
DELETE FROM thread_creation_idempotency
WHERE status = 'pending'
  AND updated_at <= ?
  AND NOT EXISTS (SELECT 1 FROM threads WHERE threads.id = thread_creation_idempotency.thread_id)
            "#,
        )
        .bind(now.saturating_sub(lease_seconds.max(0)))
        .execute(self.pool.as_ref())
        .await?;
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM thread_creation_idempotency WHERE status = 'pending'",
        )
        .fetch_one(self.pool.as_ref())
        .await?
        .try_into()?)
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::test_thread_metadata;
    use super::super::test_support::unique_temp_dir;
    use super::StateRuntime;
    use codex_protocol::ThreadId;

    #[tokio::test]
    async fn same_key_cannot_be_reused_across_start_and_fork() {
        let home = unique_temp_dir();
        let runtime = StateRuntime::init(home.clone(), "test-provider".to_string())
            .await
            .expect("runtime");
        let start_thread = ThreadId::new();
        let fork_thread = ThreadId::new();
        runtime
            .backfill_thread_creation_idempotency("client", "key", "start", start_thread)
            .await
            .expect("first binding");
        let error = runtime
            .backfill_thread_creation_idempotency("client", "key", "fork", fork_thread)
            .await
            .expect_err("cross-operation reuse must fail");
        assert!(error.to_string().contains(&start_thread.to_string()));
        let _ = tokio::fs::remove_dir_all(home).await;
    }

    #[tokio::test]
    async fn concurrent_reservations_have_exactly_one_winner() {
        let home = unique_temp_dir();
        let first = StateRuntime::init(home.clone(), "test-provider".to_string())
            .await
            .expect("first runtime");
        let second = StateRuntime::init(home.clone(), "test-provider".to_string())
            .await
            .expect("second runtime");
        let first_thread = ThreadId::new();
        let second_thread = ThreadId::new();
        let (a, b) = tokio::join!(
            first.reserve_thread_creation_idempotency("client", "key", "start", first_thread),
            second.reserve_thread_creation_idempotency("client", "key", "start", second_thread)
        );
        assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
        let binding = first
            .get_thread_creation_idempotency("client", "key")
            .await
            .expect("lookup")
            .expect("binding");
        assert!([first_thread, second_thread].contains(&binding.1));
        assert!(!binding.3);
        let _ = tokio::fs::remove_dir_all(home).await;
    }

    #[tokio::test]
    async fn fresh_pending_reservation_is_not_stolen_and_flush_recovery_commits_it() {
        let home = unique_temp_dir();
        let runtime = StateRuntime::init(home.clone(), "test-provider".to_string())
            .await
            .expect("runtime");
        let thread_id = ThreadId::new();
        runtime
            .reserve_thread_creation_idempotency("client", "key", "fork", thread_id)
            .await
            .expect("reserve");
        assert_eq!(
            runtime
                .reconcile_thread_creation_idempotency_reservations(3600)
                .await
                .expect("reconcile fresh"),
            1
        );
        runtime
            .upsert_thread(&test_thread_metadata(
                home.as_path(),
                thread_id,
                home.join("workspace"),
            ))
            .await
            .expect("canonical thread projection");
        assert_eq!(
            runtime
                .reconcile_thread_creation_idempotency_reservations(3600)
                .await
                .expect("reconcile durable"),
            0
        );
        let binding = runtime
            .get_thread_creation_idempotency("client", "key")
            .await
            .expect("lookup")
            .expect("binding");
        assert!(binding.3);
        assert_eq!(
            binding.2,
            Some(home.join(format!("rollout-{thread_id}.jsonl")))
        );
        let _ = tokio::fs::remove_dir_all(home).await;
    }

    #[tokio::test]
    async fn expired_pending_reservation_is_reclaimed_after_crash_before_flush() {
        let home = unique_temp_dir();
        let runtime = StateRuntime::init(home.clone(), "test-provider".to_string())
            .await
            .expect("runtime");
        let thread_id = ThreadId::new();
        runtime
            .reserve_thread_creation_idempotency("client", "key", "start", thread_id)
            .await
            .expect("reserve");
        assert_eq!(
            runtime
                .reconcile_thread_creation_idempotency_reservations(0)
                .await
                .expect("reconcile expired"),
            0
        );
        assert!(
            runtime
                .get_thread_creation_idempotency("client", "key")
                .await
                .expect("lookup")
                .is_none()
        );
        let _ = tokio::fs::remove_dir_all(home).await;
    }

    #[tokio::test]
    async fn hard_delete_releases_binding_but_archive_path_changes_do_not() {
        let home = unique_temp_dir();
        let runtime = StateRuntime::init(home.clone(), "test-provider".to_string())
            .await
            .expect("runtime");
        let thread_id = ThreadId::new();
        let mut metadata = test_thread_metadata(home.as_path(), thread_id, home.join("workspace"));
        runtime.upsert_thread(&metadata).await.expect("upsert");
        runtime
            .backfill_thread_creation_idempotency("client", "key", "start", thread_id)
            .await
            .expect("binding");
        metadata.rollout_path = home.join("archived_sessions/rollout.jsonl");
        runtime.upsert_thread(&metadata).await.expect("move path");
        assert_eq!(
            runtime
                .get_thread_creation_idempotency("client", "key")
                .await
                .expect("lookup")
                .expect("binding")
                .2,
            Some(metadata.rollout_path)
        );
        runtime.delete_thread(thread_id).await.expect("hard delete");
        assert!(
            runtime
                .get_thread_creation_idempotency("client", "key")
                .await
                .expect("lookup after delete")
                .is_none()
        );
        let _ = tokio::fs::remove_dir_all(home).await;
    }
}
