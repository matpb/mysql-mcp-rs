//! Server-log and activity tools: pure helpers and the SQL each source runs.
//! Caller values reach SQL only as `Bind`s; the SQL text is fixed fragments chosen by enum.

use chrono::{DateTime, Datelike, NaiveDateTime, TimeDelta, Utc};
use schemars::JsonSchema;
use serde::Deserialize;

/// One bound parameter, listed in the order its `?` appears in the SQL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Bind {
    Str(String),
    U64(u64),
}

/// Log source for `read_server_log`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum LogSource {
    /// performance_schema.error_log (MySQL 8.0.22+)
    Error,
    /// mysql.slow_log (needs slow_query_log=ON and log_output including TABLE)
    Slow,
    /// mysql.general_log (needs general_log=ON and log_output including TABLE)
    General,
}

impl LogSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Error => "error",
            Self::Slow => "slow",
            Self::General => "general",
        }
    }
}

/// Minimum error_log priority; each value includes the ones above it, Note means no filter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
pub enum MinPriority {
    System,
    Error,
    Warning,
    Note,
}

impl MinPriority {
    /// Fixed list of PRIO values at or above this priority; None means no filter.
    fn prio_list(self) -> Option<&'static str> {
        match self {
            Self::System => Some("'System'"),
            Self::Error => Some("'System','Error'"),
            Self::Warning => Some("'System','Error','Warning'"),
            Self::Note => None,
        }
    }
}

/// View for `query_activity`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ActivityView {
    /// Individual statements from performance_schema statement history.
    RecentStatements,
    /// Statement digests aggregated by performance_schema.
    TopQueries,
    /// Server errors aggregated by error number.
    ErrorSummary,
}

impl ActivityView {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RecentStatements => "recent_statements",
            Self::TopQueries => "top_queries",
            Self::ErrorSummary => "error_summary",
        }
    }
}

/// Sort key for `top_queries`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OrderBy {
    TotalTime,
    AvgTime,
    MaxTime,
    Count,
    Errors,
    RowsExamined,
}

impl OrderBy {
    fn column(self) -> &'static str {
        match self {
            Self::TotalTime => "SUM_TIMER_WAIT",
            Self::AvgTime => "AVG_TIMER_WAIT",
            Self::MaxTime => "MAX_TIMER_WAIT",
            Self::Count => "COUNT_STAR",
            Self::Errors => "SUM_ERRORS",
            Self::RowsExamined => "SUM_ROWS_EXAMINED",
        }
    }
}

/// Server facts probed on every call; nothing is kept between calls.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerFacts {
    pub version: (u32, u32, u32),
    pub current_user: String,
    pub performance_schema: bool,
    pub slow_query_log: bool,
    pub general_log: bool,
    pub log_output: String,
    pub consumers: Consumers,
    /// Whether performance_schema.prepared_statements_instances is readable; None when missing.
    pub prepared_statements: Option<bool>,
}

/// Consumer switches from performance_schema.setup_consumers; None when unreadable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Consumers {
    pub statements_current: Option<bool>,
    pub statements_history: Option<bool>,
    pub statements_history_long: Option<bool>,
    pub statements_digest: Option<bool>,
}

impl ServerFacts {
    /// Whether log_output lists TABLE, the way FIND_IN_SET('TABLE', @@log_output) reads it.
    pub fn table_output(&self) -> bool {
        self.log_output
            .split(',')
            .any(|part| part.trim().eq_ignore_ascii_case("TABLE"))
    }

    /// Recent statements read the _long history only when its consumer is known to be on.
    pub fn history_long_on(&self) -> bool {
        self.consumers.statements_history_long == Some(true)
    }
}

/// Filters and limit for `read_server_log`. since/until are already UTC strings.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LogQuery {
    pub since: Option<String>,
    pub until: Option<String>,
    pub contains: Option<String>,
    pub min_priority: Option<MinPriority>,
    pub min_query_time_ms: Option<u64>,
    pub user: Option<String>,
    pub limit: u64,
}

/// Filters and limit for `query_activity`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ActivityQuery {
    pub contains: Option<String>,
    pub schema: Option<String>,
    pub errors_only: Option<bool>,
    pub min_duration_ms: Option<u64>,
    pub order_by: Option<OrderBy>,
    pub limit: u64,
}

/// `(major, minor, patch)` from the leading digits of VERSION(): "8.4.11-google" gives (8, 4, 11).
pub fn parse_version(raw: &str) -> Option<(u32, u32, u32)> {
    let core = raw
        .split(|c: char| !(c.is_ascii_digit() || c == '.'))
        .next()?;
    let mut parts = core.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    let patch = parts.next()?.parse().ok()?;
    Some((major, minor, patch))
}

/// Converts since/until into the UTC string the builders bind: "YYYY-MM-DD HH:MM:SS.ffffff".
pub fn parse_time_bound(raw: &str, now: DateTime<Utc>) -> Result<String, String> {
    let bound = relative_bound(raw, now)
        .or_else(|| {
            NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f")
                .ok()
                .map(|dt| dt.and_utc())
        })
        .or_else(|| {
            DateTime::parse_from_rfc3339(raw)
                .ok()
                .map(|dt| dt.with_timezone(&Utc))
        })
        .filter(|dt| (1..=9999).contains(&dt.year()))
        .ok_or_else(|| {
            format!(
                "Invalid time {raw:?}: use a relative offset such as \"15m\" or \"2h\" (s, m, h, d, w; \
                 counted back from now), \"YYYY-MM-DD HH:MM:SS\" or \"YYYY-MM-DD HH:MM:SS.ffffff\" (UTC), \
                 or RFC 3339 such as \"2026-11-24T10:00:00-05:00\"."
            )
        })?;
    Ok(bound.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
}

/// "<n><unit>" with n > 0 and unit one of s, m, h, d, w, counted back from `now`.
fn relative_bound(raw: &str, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let unit_secs: i64 = match raw.chars().last()? {
        's' => 1,
        'm' => 60,
        'h' => 3_600,
        'd' => 86_400,
        'w' => 604_800,
        _ => return None,
    };
    let digits = &raw[..raw.len() - 1];
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let n: i64 = digits.parse().ok()?;
    if n == 0 {
        return None;
    }
    let delta = TimeDelta::try_seconds(n.checked_mul(unit_secs)?)?;
    now.checked_sub_signed(delta)
}

/// GRANT target for CURRENT_USER(): "user@host" becomes 'user'@'host', split on the last '@'.
pub fn grant_target(current_user: &str) -> String {
    let (user, host) = current_user.rsplit_once('@').unwrap_or((current_user, "%"));
    format!(
        "'{}'@'{}'",
        user.replace('\'', "''"),
        host.replace('\'', "''")
    )
}

/// The GRANT a DBA runs to let the MCP account read one table.
pub fn grant_line(table: &str, target: &str) -> String {
    ["GRANT SELECT ON ", table, " TO ", target, ";"].concat()
}

/// `SET PERSIST` on MySQL 8.0+; `SET GLOBAL` before it, which a restart loses.
pub fn set_line(name: &str, value: &str, version: (u32, u32, u32)) -> String {
    let verb = if version >= (8, 0, 0) {
        "SET PERSIST "
    } else {
        "SET GLOBAL "
    };
    [verb, name, " = ", value, ";"].concat()
}

/// Largest min_duration_ms accepted: one day.
pub const MAX_MIN_DURATION_MS: u64 = 86_400_000;

/// Rejects filter/source combinations that name a column the source does not have.
pub fn check_log_query(source: LogSource, q: &LogQuery) -> Result<(), String> {
    let name = source.as_str();
    if q.min_priority.is_some() && source != LogSource::Error {
        return Err(format!(
            "min_priority applies only to source \"error\", not \"{name}\"."
        ));
    }
    if q.min_query_time_ms.is_some() && source != LogSource::Slow {
        return Err(format!(
            "min_query_time_ms applies only to source \"slow\", not \"{name}\"."
        ));
    }
    if q.user.is_some() && source == LogSource::Error {
        return Err("user applies only to source \"slow\" or \"general\", not \"error\".".into());
    }
    if let (Some(since), Some(until)) = (&q.since, &q.until)
        && since > until
    {
        return Err(format!(
            "since ({since}) is later than until ({until}); swap them."
        ));
    }
    Ok(())
}

/// Rejects parameters that do not apply to the chosen view.
pub fn check_activity_query(view: ActivityView, q: &ActivityQuery) -> Result<(), String> {
    let name = view.as_str();
    if q.order_by.is_some() && view != ActivityView::TopQueries {
        return Err(format!(
            "order_by applies only to view \"top_queries\", not \"{name}\"."
        ));
    }
    if q.min_duration_ms.is_some() && view != ActivityView::RecentStatements {
        return Err(format!(
            "min_duration_ms applies only to view \"recent_statements\", not \"{name}\"."
        ));
    }
    if let Some(ms) = q.min_duration_ms
        && ms > MAX_MIN_DURATION_MS
    {
        return Err(format!(
            "min_duration_ms must be at most {MAX_MIN_DURATION_MS} (one day); got {ms}."
        ));
    }
    if view == ActivityView::ErrorSummary && (q.schema.is_some() || q.errors_only.is_some()) {
        return Err(
            "schema and errors_only apply to views \"recent_statements\" and \"top_queries\", not \"error_summary\"."
                .into(),
        );
    }
    Ok(())
}

/// Stated in the query_activity description and README too; keep the wording identical.
const HISTORY_WINDOW: &str = "History keeps the last statements per connection (performance_schema_events_statements_history_size, default 10), so this tool's own probe queries can push out earlier queries made through this server; other sessions' history is unaffected.";
/// Stated in the query_activity description and README too; keep the wording identical.
const PREPARED_TEXT_JOINED: &str = "Statements sent through the binary prepared-statement protocol show their text on the statement/com/Prepare row, joined from performance_schema.prepared_statements_instances; statement/com/Execute rows carry no text.";
const PREPARED_TEXT_UNREADABLE: &str = "Without SELECT on performance_schema.prepared_statements_instances, statements sent through the binary prepared-statement protocol show no SQL text.";

/// Notes for query_activity: what the rows can and cannot be trusted for.
pub fn activity_note(view: ActivityView, facts: &ServerFacts) -> Option<String> {
    match view {
        ActivityView::RecentStatements => {
            let prepared = if facts.prepared_statements == Some(true) {
                PREPARED_TEXT_JOINED
            } else {
                PREPARED_TEXT_UNREADABLE
            };
            let mut parts = vec![
                "SQL_TEXT is cut by the server at performance_schema_max_sql_text_length (default 1024 bytes); history keeps the last N statements per thread.".to_string(),
                prepared.to_string(),
                HISTORY_WINDOW.to_string(),
            ];
            if facts.version < (8, 0, 16) || facts.consumers.statements_current == Some(false) {
                parts.push(
                    "started_at is approximate (+/- 1 s): MySQL is older than 8.0.16, or events_statements_current is OFF.".to_string(),
                );
            }
            if !facts.history_long_on() && facts.consumers.statements_history == Some(false) {
                parts.push(
                    "events_statements_history consumer is OFF: no new statements are recorded."
                        .to_string(),
                );
            }
            Some(parts.join(" "))
        }
        ActivityView::TopQueries => (facts.consumers.statements_digest == Some(false)).then(|| {
            "statements_digest consumer is OFF: the digest table is not being updated; rows are from before it was turned off.".to_string()
        }),
        ActivityView::ErrorSummary => None,
    }
}

/// Appends `fragment` (one `?`) and its bind when the filter is set.
fn push_str_filter(sql: &mut String, binds: &mut Vec<Bind>, fragment: &str, value: Option<&str>) {
    if let Some(v) = value {
        sql.push_str(fragment);
        binds.push(Bind::Str(v.to_string()));
    }
}

pub fn error_log_sql(q: &LogQuery) -> (String, Vec<Bind>) {
    let mut sql = String::from(
        "SELECT LOGGED, THREAD_ID, PRIO, ERROR_CODE, SUBSYSTEM, DATA FROM performance_schema.error_log WHERE 1=1",
    );
    let mut binds = Vec::new();
    push_str_filter(&mut sql, &mut binds, " AND LOGGED >= ?", q.since.as_deref());
    push_str_filter(&mut sql, &mut binds, " AND LOGGED < ?", q.until.as_deref());
    push_str_filter(
        &mut sql,
        &mut binds,
        " AND INSTR(DATA, ?) > 0",
        q.contains.as_deref(),
    );
    if let Some(list) = q.min_priority.and_then(MinPriority::prio_list) {
        sql.push_str(" AND PRIO IN (");
        sql.push_str(list);
        sql.push(')');
    }
    sql.push_str(" ORDER BY LOGGED DESC LIMIT ?");
    binds.push(Bind::U64(q.limit));
    (sql, binds)
}

pub fn slow_log_sql(q: &LogQuery) -> (String, Vec<Bind>) {
    let mut sql = String::from(
        "SELECT start_time, user_host, query_time, lock_time, rows_sent, rows_examined, db, CONVERT(sql_text USING utf8mb4) AS sql_text, thread_id FROM mysql.slow_log WHERE 1=1",
    );
    let mut binds = Vec::new();
    push_str_filter(
        &mut sql,
        &mut binds,
        " AND start_time >= ?",
        q.since.as_deref(),
    );
    push_str_filter(
        &mut sql,
        &mut binds,
        " AND start_time < ?",
        q.until.as_deref(),
    );
    push_str_filter(
        &mut sql,
        &mut binds,
        " AND INSTR(CONVERT(sql_text USING utf8mb4), ?) > 0",
        q.contains.as_deref(),
    );
    push_str_filter(
        &mut sql,
        &mut binds,
        " AND INSTR(user_host, ?) > 0",
        q.user.as_deref(),
    );
    if let Some(ms) = q.min_query_time_ms {
        sql.push_str(" AND query_time >= SEC_TO_TIME(? / 1000)");
        binds.push(Bind::U64(ms));
    }
    sql.push_str(" ORDER BY start_time DESC LIMIT ?");
    binds.push(Bind::U64(q.limit));
    (sql, binds)
}

pub fn general_log_sql(q: &LogQuery) -> (String, Vec<Bind>) {
    let mut sql = String::from(
        "SELECT event_time, user_host, thread_id, command_type, CONVERT(argument USING utf8mb4) AS argument FROM mysql.general_log WHERE 1=1",
    );
    let mut binds = Vec::new();
    push_str_filter(
        &mut sql,
        &mut binds,
        " AND event_time >= ?",
        q.since.as_deref(),
    );
    push_str_filter(
        &mut sql,
        &mut binds,
        " AND event_time < ?",
        q.until.as_deref(),
    );
    push_str_filter(
        &mut sql,
        &mut binds,
        " AND INSTR(CONVERT(argument USING utf8mb4), ?) > 0",
        q.contains.as_deref(),
    );
    push_str_filter(
        &mut sql,
        &mut binds,
        " AND INSTR(user_host, ?) > 0",
        q.user.as_deref(),
    );
    sql.push_str(" ORDER BY event_time DESC LIMIT ?");
    binds.push(Bind::U64(q.limit));
    (sql, binds)
}

/// Anchor pairs the server clock (picoseconds) with TIMER_START. 8.0.16+ prefers the statement
/// running now and falls back to Uptime; older servers use Uptime only. Both always yield one row.
const ANCHOR_CURRENT: &str = " CROSS JOIN (SELECT COALESCE((SELECT TIMER_START FROM performance_schema.events_statements_current WHERE THREAD_ID = PS_CURRENT_THREAD_ID()), (SELECT VARIABLE_VALUE * 1000000000000 FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Uptime')) AS ts) anchor";
/// Joins the Prepare row to its statement text (only when the table is readable).
const PREPARED_JOIN: &str = " LEFT JOIN performance_schema.prepared_statements_instances p ON p.OWNER_THREAD_ID = h.THREAD_ID AND p.OWNER_EVENT_ID = h.EVENT_ID";
const ANCHOR_UPTIME: &str = " CROSS JOIN (SELECT (SELECT VARIABLE_VALUE * 1000000000000 FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Uptime') AS ts) anchor";

pub fn recent_statements_sql(q: &ActivityQuery, facts: &ServerFacts) -> (String, Vec<Bind>) {
    let table = if facts.history_long_on() {
        "events_statements_history_long"
    } else {
        "events_statements_history"
    };
    let anchor = if facts.version >= (8, 0, 16) {
        ANCHOR_CURRENT
    } else {
        ANCHOR_UPTIME
    };
    // Binary-protocol statements keep their text on the Prepare row, in prepared_statements_instances.
    let psi = facts.prepared_statements == Some(true);
    let text = if psi {
        "COALESCE(h.SQL_TEXT, p.SQL_TEXT)"
    } else {
        "h.SQL_TEXT"
    };
    let mut sql = String::from(
        "SELECT DATE_FORMAT(NOW(6) - INTERVAL ((anchor.ts - h.TIMER_START) DIV 1000000) MICROSECOND, '%Y-%m-%d %H:%i:%s.%fZ') AS started_at, h.THREAD_ID, h.EVENT_ID, h.EVENT_NAME, h.CURRENT_SCHEMA, ",
    );
    sql.push_str(if psi {
        "COALESCE(h.SQL_TEXT, p.SQL_TEXT) AS SQL_TEXT"
    } else {
        "h.SQL_TEXT"
    });
    sql.push_str(", ROUND(h.TIMER_WAIT / 1000000000, 3) AS duration_ms, ROUND(h.LOCK_TIME / 1000000000, 3) AS lock_ms, h.ROWS_SENT, h.ROWS_EXAMINED, h.ROWS_AFFECTED, h.ERRORS, h.WARNINGS, h.MYSQL_ERRNO, h.RETURNED_SQLSTATE, h.MESSAGE_TEXT FROM performance_schema.");
    sql.push_str(table);
    sql.push_str(" h");
    sql.push_str(anchor);
    if psi {
        sql.push_str(PREPARED_JOIN);
    }
    // Ping rows carry no statement and would flood the view.
    sql.push_str(" WHERE 1=1 AND h.EVENT_NAME <> 'statement/com/Ping'");
    let mut binds = Vec::new();
    if q.errors_only == Some(true) {
        sql.push_str(" AND h.ERRORS > 0");
    }
    if let Some(ms) = q.min_duration_ms {
        sql.push_str(" AND h.TIMER_WAIT >= ? * 1000000000");
        binds.push(Bind::U64(ms));
    }
    // A failed Prepare has no text, only its error message, so both are searched.
    if let Some(v) = q.contains.as_deref() {
        sql.push_str(" AND (INSTR(");
        sql.push_str(text);
        sql.push_str(", ?) > 0 OR INSTR(h.MESSAGE_TEXT, ?) > 0)");
        binds.push(Bind::Str(v.to_string()));
        binds.push(Bind::Str(v.to_string()));
    }
    push_str_filter(
        &mut sql,
        &mut binds,
        " AND h.CURRENT_SCHEMA = ?",
        q.schema.as_deref(),
    );
    sql.push_str(" ORDER BY h.TIMER_START DESC LIMIT ?");
    binds.push(Bind::U64(q.limit));
    (sql, binds)
}

pub fn top_queries_sql(q: &ActivityQuery, facts: &ServerFacts) -> (String, Vec<Bind>) {
    let mut sql = String::from(
        "SELECT SCHEMA_NAME, DIGEST, DIGEST_TEXT, COUNT_STAR AS exec_count, ROUND(SUM_TIMER_WAIT / 1000000000, 3) AS total_ms, ROUND(AVG_TIMER_WAIT / 1000000000, 3) AS avg_ms, ROUND(MAX_TIMER_WAIT / 1000000000, 3) AS max_ms, SUM_ERRORS, SUM_WARNINGS, SUM_ROWS_SENT, SUM_ROWS_EXAMINED, SUM_NO_INDEX_USED, FIRST_SEEN, LAST_SEEN",
    );
    if facts.version.0 >= 8 {
        sql.push_str(", QUERY_SAMPLE_TEXT");
    }
    sql.push_str(
        " FROM performance_schema.events_statements_summary_by_digest WHERE DIGEST IS NOT NULL",
    );
    let mut binds = Vec::new();
    push_str_filter(
        &mut sql,
        &mut binds,
        " AND INSTR(DIGEST_TEXT, ?) > 0",
        q.contains.as_deref(),
    );
    push_str_filter(
        &mut sql,
        &mut binds,
        " AND SCHEMA_NAME = ?",
        q.schema.as_deref(),
    );
    if q.errors_only == Some(true) {
        sql.push_str(" AND SUM_ERRORS > 0");
    }
    sql.push_str(" ORDER BY ");
    sql.push_str(q.order_by.unwrap_or(OrderBy::TotalTime).column());
    sql.push_str(" DESC LIMIT ?");
    binds.push(Bind::U64(q.limit));
    (sql, binds)
}

pub fn error_summary_sql(q: &ActivityQuery) -> (String, Vec<Bind>) {
    let mut sql = String::from(
        "SELECT ERROR_NUMBER, ERROR_NAME, SQL_STATE, SUM_ERROR_RAISED, SUM_ERROR_HANDLED, FIRST_SEEN, LAST_SEEN FROM performance_schema.events_errors_summary_global_by_error WHERE SUM_ERROR_RAISED > 0",
    );
    let mut binds = Vec::new();
    push_str_filter(
        &mut sql,
        &mut binds,
        " AND INSTR(ERROR_NAME, ?) > 0",
        q.contains.as_deref(),
    );
    sql.push_str(" ORDER BY SUM_ERROR_RAISED DESC LIMIT ?");
    binds.push(Bind::U64(q.limit));
    (sql, binds)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 11, 24, 12, 0, 0).unwrap()
    }

    fn facts(version: (u32, u32, u32), long_on: bool) -> ServerFacts {
        ServerFacts {
            version,
            current_user: "report_ro@%".into(),
            performance_schema: true,
            slow_query_log: false,
            general_log: false,
            log_output: "FILE".into(),
            prepared_statements: None,
            consumers: Consumers {
                statements_current: Some(true),
                statements_history: Some(true),
                statements_history_long: Some(long_on),
                statements_digest: Some(true),
            },
        }
    }

    fn s(v: &str) -> Bind {
        Bind::Str(v.into())
    }

    #[test]
    fn parse_version_reads_leading_digits() {
        assert_eq!(parse_version("8.4.11-google"), Some((8, 4, 11)));
        assert_eq!(parse_version("5.7.44-log"), Some((5, 7, 44)));
        assert_eq!(parse_version("garbage"), None);
        assert_eq!(parse_version(""), None);
        assert_eq!(parse_version("8.0"), None);
    }

    #[test]
    fn relative_bounds_count_back_from_now() {
        assert_eq!(
            parse_time_bound("15m", now()).unwrap(),
            "2026-11-24 11:45:00.000000"
        );
        assert_eq!(
            parse_time_bound("2h", now()).unwrap(),
            "2026-11-24 10:00:00.000000"
        );
        assert_eq!(
            parse_time_bound("7d", now()).unwrap(),
            "2026-11-17 12:00:00.000000"
        );
        assert_eq!(
            parse_time_bound("1w", now()).unwrap(),
            "2026-11-17 12:00:00.000000"
        );
        assert_eq!(
            parse_time_bound("30s", now()).unwrap(),
            "2026-11-24 11:59:30.000000"
        );
    }

    #[test]
    fn absolute_and_rfc3339_bounds_are_utc() {
        assert_eq!(
            parse_time_bound("2026-11-24 10:00:00", now()).unwrap(),
            "2026-11-24 10:00:00.000000"
        );
        assert_eq!(
            parse_time_bound("2026-11-24 10:00:00.5", now()).unwrap(),
            "2026-11-24 10:00:00.500000"
        );
        assert_eq!(
            parse_time_bound("2026-11-24 10:00:00.123456", now()).unwrap(),
            "2026-11-24 10:00:00.123456"
        );
        assert_eq!(
            parse_time_bound("2026-11-24T10:00:00-05:00", now()).unwrap(),
            "2026-11-24 15:00:00.000000"
        );
    }

    #[test]
    fn bad_time_bounds_are_rejected_with_examples() {
        for raw in [
            "0m",
            "-5m",
            "5x",
            "",
            "1.5h",
            "1000000000000w",
            "99999999999999999999s",
        ] {
            let err = parse_time_bound(raw, now()).unwrap_err();
            assert!(err.contains("\"15m\""), "{raw}: {err}");
            assert!(err.contains("RFC 3339"), "{raw}: {err}");
        }
    }

    #[test]
    fn grant_target_quotes_user_and_host() {
        assert_eq!(grant_target("report_ro@%"), "'report_ro'@'%'");
        assert_eq!(grant_target("a@b@host"), "'a@b'@'host'");
        assert_eq!(grant_target("o'neil@%"), "'o''neil'@'%'");
        assert_eq!(
            grant_line("mysql.slow_log", &grant_target("report_ro@%")),
            "GRANT SELECT ON mysql.slow_log TO 'report_ro'@'%';"
        );
    }

    #[test]
    fn table_output_reads_the_log_output_list() {
        let mut f = facts((8, 0, 36), false);
        f.log_output = "TABLE,FILE".into();
        assert!(f.table_output());
        f.log_output = "FILE".into();
        assert!(!f.table_output());
        f.log_output = "FILE,TABLE".into();
        assert!(f.table_output());
    }

    #[test]
    fn error_log_sql_without_and_with_filters() {
        let bare = LogQuery {
            limit: 101,
            ..Default::default()
        };
        assert_eq!(
            error_log_sql(&bare),
            (
                "SELECT LOGGED, THREAD_ID, PRIO, ERROR_CODE, SUBSYSTEM, DATA FROM performance_schema.error_log WHERE 1=1 ORDER BY LOGGED DESC LIMIT ?".to_string(),
                vec![Bind::U64(101)]
            )
        );
        let full = LogQuery {
            since: Some("2026-11-24 09:00:00.000000".into()),
            until: Some("2026-11-24 10:00:00.000000".into()),
            contains: Some("deadlock".into()),
            min_priority: Some(MinPriority::Warning),
            limit: 101,
            ..Default::default()
        };
        assert_eq!(
            error_log_sql(&full),
            (
                "SELECT LOGGED, THREAD_ID, PRIO, ERROR_CODE, SUBSYSTEM, DATA FROM performance_schema.error_log WHERE 1=1 AND LOGGED >= ? AND LOGGED < ? AND INSTR(DATA, ?) > 0 AND PRIO IN ('System','Error','Warning') ORDER BY LOGGED DESC LIMIT ?".to_string(),
                vec![
                    s("2026-11-24 09:00:00.000000"),
                    s("2026-11-24 10:00:00.000000"),
                    s("deadlock"),
                    Bind::U64(101)
                ]
            )
        );
    }

    #[test]
    fn min_priority_maps_to_fixed_lists() {
        let list = |p| {
            let q = LogQuery {
                min_priority: Some(p),
                limit: 1,
                ..Default::default()
            };
            error_log_sql(&q).0
        };
        assert!(
            list(MinPriority::System)
                .ends_with("AND PRIO IN ('System') ORDER BY LOGGED DESC LIMIT ?")
        );
        assert!(list(MinPriority::Error).contains("AND PRIO IN ('System','Error') "));
        assert!(list(MinPriority::Note).ends_with("WHERE 1=1 ORDER BY LOGGED DESC LIMIT ?"));
    }

    #[test]
    fn slow_log_sql_without_and_with_filters() {
        let bare = LogQuery {
            limit: 101,
            ..Default::default()
        };
        assert_eq!(
            slow_log_sql(&bare),
            (
                "SELECT start_time, user_host, query_time, lock_time, rows_sent, rows_examined, db, CONVERT(sql_text USING utf8mb4) AS sql_text, thread_id FROM mysql.slow_log WHERE 1=1 ORDER BY start_time DESC LIMIT ?".to_string(),
                vec![Bind::U64(101)]
            )
        );
        let full = LogQuery {
            since: Some("2026-11-24 09:00:00.000000".into()),
            until: Some("2026-11-24 10:00:00.000000".into()),
            contains: Some("SELECT".into()),
            user: Some("app@%".into()),
            min_query_time_ms: Some(1500),
            limit: 101,
            ..Default::default()
        };
        assert_eq!(
            slow_log_sql(&full),
            (
                "SELECT start_time, user_host, query_time, lock_time, rows_sent, rows_examined, db, CONVERT(sql_text USING utf8mb4) AS sql_text, thread_id FROM mysql.slow_log WHERE 1=1 AND start_time >= ? AND start_time < ? AND INSTR(CONVERT(sql_text USING utf8mb4), ?) > 0 AND INSTR(user_host, ?) > 0 AND query_time >= SEC_TO_TIME(? / 1000) ORDER BY start_time DESC LIMIT ?".to_string(),
                vec![
                    s("2026-11-24 09:00:00.000000"),
                    s("2026-11-24 10:00:00.000000"),
                    s("SELECT"),
                    s("app@%"),
                    Bind::U64(1500),
                    Bind::U64(101)
                ]
            )
        );
    }

    #[test]
    fn general_log_sql_without_and_with_filters() {
        let bare = LogQuery {
            limit: 101,
            ..Default::default()
        };
        assert_eq!(
            general_log_sql(&bare),
            (
                "SELECT event_time, user_host, thread_id, command_type, CONVERT(argument USING utf8mb4) AS argument FROM mysql.general_log WHERE 1=1 ORDER BY event_time DESC LIMIT ?".to_string(),
                vec![Bind::U64(101)]
            )
        );
        let full = LogQuery {
            since: Some("2026-11-24 09:00:00.000000".into()),
            until: Some("2026-11-24 10:00:00.000000".into()),
            contains: Some("SELECT".into()),
            user: Some("app@%".into()),
            limit: 101,
            ..Default::default()
        };
        assert_eq!(
            general_log_sql(&full),
            (
                "SELECT event_time, user_host, thread_id, command_type, CONVERT(argument USING utf8mb4) AS argument FROM mysql.general_log WHERE 1=1 AND event_time >= ? AND event_time < ? AND INSTR(CONVERT(argument USING utf8mb4), ?) > 0 AND INSTR(user_host, ?) > 0 ORDER BY event_time DESC LIMIT ?".to_string(),
                vec![
                    s("2026-11-24 09:00:00.000000"),
                    s("2026-11-24 10:00:00.000000"),
                    s("SELECT"),
                    s("app@%"),
                    Bind::U64(101)
                ]
            )
        );
    }

    const RECENT_HEAD: &str = "SELECT DATE_FORMAT(NOW(6) - INTERVAL ((anchor.ts - h.TIMER_START) DIV 1000000) MICROSECOND, '%Y-%m-%d %H:%i:%s.%fZ') AS started_at, h.THREAD_ID, h.EVENT_ID, h.EVENT_NAME, h.CURRENT_SCHEMA, h.SQL_TEXT, ROUND(h.TIMER_WAIT / 1000000000, 3) AS duration_ms, ROUND(h.LOCK_TIME / 1000000000, 3) AS lock_ms, h.ROWS_SENT, h.ROWS_EXAMINED, h.ROWS_AFFECTED, h.ERRORS, h.WARNINGS, h.MYSQL_ERRNO, h.RETURNED_SQLSTATE, h.MESSAGE_TEXT FROM performance_schema.";
    const RECENT_HEAD_PSI: &str = "SELECT DATE_FORMAT(NOW(6) - INTERVAL ((anchor.ts - h.TIMER_START) DIV 1000000) MICROSECOND, '%Y-%m-%d %H:%i:%s.%fZ') AS started_at, h.THREAD_ID, h.EVENT_ID, h.EVENT_NAME, h.CURRENT_SCHEMA, COALESCE(h.SQL_TEXT, p.SQL_TEXT) AS SQL_TEXT, ROUND(h.TIMER_WAIT / 1000000000, 3) AS duration_ms, ROUND(h.LOCK_TIME / 1000000000, 3) AS lock_ms, h.ROWS_SENT, h.ROWS_EXAMINED, h.ROWS_AFFECTED, h.ERRORS, h.WARNINGS, h.MYSQL_ERRNO, h.RETURNED_SQLSTATE, h.MESSAGE_TEXT FROM performance_schema.";
    const PSI_JOIN: &str = " LEFT JOIN performance_schema.prepared_statements_instances p ON p.OWNER_THREAD_ID = h.THREAD_ID AND p.OWNER_EVENT_ID = h.EVENT_ID";
    const CURRENT_ANCHOR: &str = " CROSS JOIN (SELECT COALESCE((SELECT TIMER_START FROM performance_schema.events_statements_current WHERE THREAD_ID = PS_CURRENT_THREAD_ID()), (SELECT VARIABLE_VALUE * 1000000000000 FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Uptime')) AS ts) anchor";
    const UPTIME_ANCHOR: &str = " CROSS JOIN (SELECT (SELECT VARIABLE_VALUE * 1000000000000 FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Uptime') AS ts) anchor";

    #[test]
    fn recent_statements_note_flags_approximate_and_off_consumers() {
        let note = |version, current: Option<bool>, history: Option<bool>, long: bool| {
            let mut f = facts(version, long);
            f.prepared_statements = Some(true);
            f.consumers.statements_current = current;
            f.consumers.statements_history = history;
            activity_note(ActivityView::RecentStatements, &f).unwrap()
        };
        let cut = "SQL_TEXT is cut by the server at performance_schema_max_sql_text_length (default 1024 bytes); history keeps the last N statements per thread.";
        let joined = "Statements sent through the binary prepared-statement protocol show their text on the statement/com/Prepare row, joined from performance_schema.prepared_statements_instances; statement/com/Execute rows carry no text.";
        let history = "History keeps the last statements per connection (performance_schema_events_statements_history_size, default 10), so this tool's own probe queries can push out earlier queries made through this server; other sessions' history is unaffected.";
        let base = format!("{cut} {joined} {history}");
        let approx = "started_at is approximate (+/- 1 s): MySQL is older than 8.0.16, or events_statements_current is OFF.";
        let off = "events_statements_history consumer is OFF: no new statements are recorded.";
        assert_eq!(note((8, 0, 16), Some(true), Some(true), false), base);
        assert_eq!(
            note((5, 7, 44), Some(true), Some(true), false),
            format!("{base} {approx}")
        );
        assert_eq!(
            note((8, 0, 36), Some(true), Some(true), false),
            base.clone()
        );
        assert_eq!(
            note((8, 0, 36), Some(false), Some(true), false),
            format!("{base} {approx}")
        );
        assert_eq!(
            note((8, 0, 36), Some(true), Some(false), false),
            format!("{base} {off}")
        );
        // The _long history is the one read when its consumer is on, so history OFF is moot.
        assert_eq!(note((8, 0, 36), Some(true), Some(false), true), base);
    }

    #[test]
    fn prepared_text_note_matches_what_is_readable() {
        let cut = "SQL_TEXT is cut by the server at performance_schema_max_sql_text_length (default 1024 bytes); history keeps the last N statements per thread.";
        let history = "History keeps the last statements per connection (performance_schema_events_statements_history_size, default 10), so this tool's own probe queries can push out earlier queries made through this server; other sessions' history is unaffected.";
        let mut f = facts((8, 0, 36), false);
        f.prepared_statements = None;
        assert_eq!(
            activity_note(ActivityView::RecentStatements, &f).unwrap(),
            format!(
                "{cut} Without SELECT on performance_schema.prepared_statements_instances, statements sent through the binary prepared-statement protocol show no SQL text. {history}"
            )
        );
    }

    #[test]
    fn set_line_uses_persist_on_8_0_and_global_before() {
        assert_eq!(
            set_line("slow_query_log", "ON", (8, 0, 36)),
            "SET PERSIST slow_query_log = ON;"
        );
        assert_eq!(
            set_line("log_output", "'FILE,TABLE'", (5, 7, 44)),
            "SET GLOBAL log_output = 'FILE,TABLE';"
        );
    }

    #[test]
    fn min_duration_is_bounded_to_one_day() {
        let at = |ms| ActivityQuery {
            min_duration_ms: Some(ms),
            limit: 1,
            ..Default::default()
        };
        assert!(check_activity_query(ActivityView::RecentStatements, &at(86_400_000)).is_ok());
        let err =
            check_activity_query(ActivityView::RecentStatements, &at(86_400_001)).unwrap_err();
        assert!(err.contains("min_duration_ms"), "{err}");
        assert!(err.contains("86400000"), "{err}");
    }

    #[test]
    fn top_queries_note_flags_a_stopped_digest_consumer() {
        let mut f = facts((8, 0, 36), false);
        assert_eq!(activity_note(ActivityView::TopQueries, &f), None);
        f.consumers.statements_digest = Some(false);
        assert_eq!(
            activity_note(ActivityView::TopQueries, &f).as_deref(),
            Some(
                "statements_digest consumer is OFF: the digest table is not being updated; rows are from before it was turned off."
            )
        );
        f.consumers.statements_digest = None;
        assert_eq!(activity_note(ActivityView::TopQueries, &f), None);
    }

    #[test]
    fn recent_statements_uses_current_thread_anchor_on_8_0_16() {
        let q = ActivityQuery {
            limit: 101,
            ..Default::default()
        };
        let expected = format!(
            "{RECENT_HEAD}events_statements_history h{CURRENT_ANCHOR} WHERE 1=1 AND h.EVENT_NAME <> 'statement/com/Ping' ORDER BY h.TIMER_START DESC LIMIT ?"
        );
        assert_eq!(
            recent_statements_sql(&q, &facts((8, 0, 16), false)),
            (expected, vec![Bind::U64(101)])
        );
    }

    #[test]
    fn recent_statements_uses_uptime_anchor_before_8_0_16_and_long_history_when_on() {
        let q = ActivityQuery {
            errors_only: Some(true),
            min_duration_ms: Some(250),
            contains: Some("SELECT".into()),
            schema: Some("app".into()),
            limit: 101,
            ..Default::default()
        };
        let expected = format!(
            "{RECENT_HEAD}events_statements_history_long h{UPTIME_ANCHOR} WHERE 1=1 AND h.EVENT_NAME <> 'statement/com/Ping' AND h.ERRORS > 0 AND h.TIMER_WAIT >= ? * 1000000000 AND (INSTR(h.SQL_TEXT, ?) > 0 OR INSTR(h.MESSAGE_TEXT, ?) > 0) AND h.CURRENT_SCHEMA = ? ORDER BY h.TIMER_START DESC LIMIT ?"
        );
        assert_eq!(
            recent_statements_sql(&q, &facts((8, 0, 15), true)),
            (
                expected,
                vec![
                    Bind::U64(250),
                    s("SELECT"),
                    s("SELECT"),
                    s("app"),
                    Bind::U64(101)
                ]
            )
        );
        let five_seven = format!(
            "{RECENT_HEAD}events_statements_history h{UPTIME_ANCHOR} WHERE 1=1 AND h.EVENT_NAME <> 'statement/com/Ping' ORDER BY h.TIMER_START DESC LIMIT ?"
        );
        let bare = ActivityQuery {
            limit: 101,
            ..Default::default()
        };
        assert_eq!(
            recent_statements_sql(&bare, &facts((5, 7, 44), false)),
            (five_seven, vec![Bind::U64(101)])
        );
    }

    #[test]
    fn recent_statements_reads_prepared_text_through_the_join_when_readable() {
        let bare = ActivityQuery {
            limit: 101,
            ..Default::default()
        };
        let mut f = facts((8, 0, 16), false);
        f.prepared_statements = Some(true);
        assert_eq!(
            recent_statements_sql(&bare, &f),
            (
                format!(
                    "{RECENT_HEAD_PSI}events_statements_history h{CURRENT_ANCHOR}{PSI_JOIN} WHERE 1=1 AND h.EVENT_NAME <> 'statement/com/Ping' ORDER BY h.TIMER_START DESC LIMIT ?"
                ),
                vec![Bind::U64(101)]
            )
        );

        let full = ActivityQuery {
            errors_only: Some(true),
            min_duration_ms: Some(250),
            contains: Some("SELECT".into()),
            schema: Some("app".into()),
            limit: 101,
            ..Default::default()
        };
        let mut f = facts((8, 0, 15), true);
        f.prepared_statements = Some(true);
        assert_eq!(
            recent_statements_sql(&full, &f),
            (
                format!(
                    "{RECENT_HEAD_PSI}events_statements_history_long h{UPTIME_ANCHOR}{PSI_JOIN} WHERE 1=1 AND h.EVENT_NAME <> 'statement/com/Ping' AND h.ERRORS > 0 AND h.TIMER_WAIT >= ? * 1000000000 AND (INSTR(COALESCE(h.SQL_TEXT, p.SQL_TEXT), ?) > 0 OR INSTR(h.MESSAGE_TEXT, ?) > 0) AND h.CURRENT_SCHEMA = ? ORDER BY h.TIMER_START DESC LIMIT ?"
                ),
                vec![
                    Bind::U64(250),
                    s("SELECT"),
                    s("SELECT"),
                    s("app"),
                    Bind::U64(101)
                ]
            )
        );
    }

    const TOP_HEAD: &str = "SELECT SCHEMA_NAME, DIGEST, DIGEST_TEXT, COUNT_STAR AS exec_count, ROUND(SUM_TIMER_WAIT / 1000000000, 3) AS total_ms, ROUND(AVG_TIMER_WAIT / 1000000000, 3) AS avg_ms, ROUND(MAX_TIMER_WAIT / 1000000000, 3) AS max_ms, SUM_ERRORS, SUM_WARNINGS, SUM_ROWS_SENT, SUM_ROWS_EXAMINED, SUM_NO_INDEX_USED, FIRST_SEEN, LAST_SEEN";

    #[test]
    fn top_queries_sql_with_and_without_filters() {
        let bare = ActivityQuery {
            limit: 101,
            ..Default::default()
        };
        assert_eq!(
            top_queries_sql(&bare, &facts((8, 0, 36), false)),
            (
                format!(
                    "{TOP_HEAD}, QUERY_SAMPLE_TEXT FROM performance_schema.events_statements_summary_by_digest WHERE DIGEST IS NOT NULL ORDER BY SUM_TIMER_WAIT DESC LIMIT ?"
                ),
                vec![Bind::U64(101)]
            )
        );
        let full = ActivityQuery {
            contains: Some("users".into()),
            schema: Some("app".into()),
            errors_only: Some(true),
            order_by: Some(OrderBy::Errors),
            limit: 101,
            ..Default::default()
        };
        assert_eq!(
            top_queries_sql(&full, &facts((8, 0, 36), false)),
            (
                format!(
                    "{TOP_HEAD}, QUERY_SAMPLE_TEXT FROM performance_schema.events_statements_summary_by_digest WHERE DIGEST IS NOT NULL AND INSTR(DIGEST_TEXT, ?) > 0 AND SCHEMA_NAME = ? AND SUM_ERRORS > 0 ORDER BY SUM_ERRORS DESC LIMIT ?"
                ),
                vec![s("users"), s("app"), Bind::U64(101)]
            )
        );
    }

    #[test]
    fn top_queries_has_sample_text_only_on_8_0() {
        let q = ActivityQuery {
            limit: 1,
            ..Default::default()
        };
        let (on_8, _) = top_queries_sql(&q, &facts((8, 0, 16), false));
        let (on_5, _) = top_queries_sql(&q, &facts((5, 7, 44), false));
        assert!(on_8.contains("QUERY_SAMPLE_TEXT FROM"));
        assert!(!on_5.contains("QUERY_SAMPLE_TEXT"));
    }

    #[test]
    fn top_queries_order_by_maps_each_variant_to_its_column() {
        let cases = [
            (OrderBy::TotalTime, "SUM_TIMER_WAIT"),
            (OrderBy::AvgTime, "AVG_TIMER_WAIT"),
            (OrderBy::MaxTime, "MAX_TIMER_WAIT"),
            (OrderBy::Count, "COUNT_STAR"),
            (OrderBy::Errors, "SUM_ERRORS"),
            (OrderBy::RowsExamined, "SUM_ROWS_EXAMINED"),
        ];
        for (order, column) in cases {
            let q = ActivityQuery {
                order_by: Some(order),
                limit: 5,
                ..Default::default()
            };
            let (sql, _) = top_queries_sql(&q, &facts((8, 0, 36), false));
            assert!(
                sql.ends_with(&format!(" ORDER BY {column} DESC LIMIT ?")),
                "{order:?}: {sql}"
            );
        }
    }

    #[test]
    fn error_summary_sql_with_and_without_filters() {
        let bare = ActivityQuery {
            limit: 101,
            ..Default::default()
        };
        assert_eq!(
            error_summary_sql(&bare),
            (
                "SELECT ERROR_NUMBER, ERROR_NAME, SQL_STATE, SUM_ERROR_RAISED, SUM_ERROR_HANDLED, FIRST_SEEN, LAST_SEEN FROM performance_schema.events_errors_summary_global_by_error WHERE SUM_ERROR_RAISED > 0 ORDER BY SUM_ERROR_RAISED DESC LIMIT ?".to_string(),
                vec![Bind::U64(101)]
            )
        );
        let full = ActivityQuery {
            contains: Some("lock".into()),
            limit: 101,
            ..Default::default()
        };
        assert_eq!(
            error_summary_sql(&full),
            (
                "SELECT ERROR_NUMBER, ERROR_NAME, SQL_STATE, SUM_ERROR_RAISED, SUM_ERROR_HANDLED, FIRST_SEEN, LAST_SEEN FROM performance_schema.events_errors_summary_global_by_error WHERE SUM_ERROR_RAISED > 0 AND INSTR(ERROR_NAME, ?) > 0 ORDER BY SUM_ERROR_RAISED DESC LIMIT ?".to_string(),
                vec![s("lock"), Bind::U64(101)]
            )
        );
    }

    #[test]
    fn caller_values_only_reach_binds() {
        let hostile = "x'; DROP TABLE t; --";
        let q = LogQuery {
            contains: Some(hostile.into()),
            user: Some(hostile.into()),
            limit: 1,
            ..Default::default()
        };
        for (sql, binds) in [slow_log_sql(&q), general_log_sql(&q), error_log_sql(&q)] {
            assert!(!sql.contains("DROP"), "{sql}");
            assert!(binds.contains(&s(hostile)));
        }
    }

    #[test]
    fn min_priority_with_slow_source_is_rejected_by_name() {
        let q = LogQuery {
            min_priority: Some(MinPriority::Error),
            limit: 1,
            ..Default::default()
        };
        let err = check_log_query(LogSource::Slow, &q).unwrap_err();
        assert!(err.contains("min_priority"), "{err}");
        assert!(err.contains("\"slow\""), "{err}");
        assert!(check_log_query(LogSource::Error, &q).is_ok());
    }

    #[test]
    fn user_and_min_query_time_name_their_source() {
        let user = LogQuery {
            user: Some("app".into()),
            limit: 1,
            ..Default::default()
        };
        assert!(
            check_log_query(LogSource::Error, &user)
                .unwrap_err()
                .contains("user")
        );
        assert!(check_log_query(LogSource::General, &user).is_ok());
        let qt = LogQuery {
            min_query_time_ms: Some(10),
            limit: 1,
            ..Default::default()
        };
        assert!(
            check_log_query(LogSource::General, &qt)
                .unwrap_err()
                .contains("min_query_time_ms")
        );
        assert!(check_log_query(LogSource::Slow, &qt).is_ok());
    }

    #[test]
    fn since_after_until_is_rejected() {
        let q = LogQuery {
            since: Some("2026-11-24 10:00:00.000000".into()),
            until: Some("2026-11-24 09:00:00.000000".into()),
            limit: 1,
            ..Default::default()
        };
        assert!(check_log_query(LogSource::Slow, &q).is_err());
        let ok = LogQuery {
            since: Some("2026-11-24 09:00:00.000000".into()),
            until: Some("2026-11-24 10:00:00.000000".into()),
            limit: 1,
            ..Default::default()
        };
        assert!(check_log_query(LogSource::Slow, &ok).is_ok());
    }

    #[test]
    fn order_by_with_recent_statements_is_rejected_by_name() {
        let q = ActivityQuery {
            order_by: Some(OrderBy::Count),
            limit: 1,
            ..Default::default()
        };
        let err = check_activity_query(ActivityView::RecentStatements, &q).unwrap_err();
        assert!(err.contains("order_by"), "{err}");
        assert!(check_activity_query(ActivityView::TopQueries, &q).is_ok());
    }
}
