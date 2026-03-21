//! Integration-style tests for the query sanitizer (read-only enforcement).

use mysql_mcp::sanitizer::{apply_limit, sanitize};

#[test]
fn select_allowed() {
    let r = sanitize("SELECT * FROM users");
    assert!(r.is_valid);
}

#[test]
fn insert_blocked() {
    let r = sanitize("INSERT INTO users VALUES (1, 'test')");
    assert!(!r.is_valid);
}

#[test]
fn drop_blocked() {
    let r = sanitize("DROP TABLE users");
    assert!(!r.is_valid);
}

#[test]
fn show_allowed() {
    let r = sanitize("SHOW TABLES");
    assert!(r.is_valid);
}

#[test]
fn describe_allowed() {
    let r = sanitize("DESCRIBE users");
    assert!(r.is_valid);
}

#[test]
fn multi_statement_blocked() {
    let r = sanitize("SELECT 1; DROP TABLE users");
    assert!(!r.is_valid);
}

#[test]
fn trailing_semicolon_ok() {
    let r = sanitize("SELECT 1;");
    assert!(r.is_valid);
}

#[test]
fn into_outfile_blocked() {
    let r = sanitize("SELECT * FROM users INTO OUTFILE '/tmp/data'");
    assert!(!r.is_valid);
}

#[test]
fn for_update_blocked() {
    let r = sanitize("SELECT * FROM users FOR UPDATE");
    assert!(!r.is_valid);
}

#[test]
fn set_session_var_allowed() {
    let r = sanitize("SET @foo = 1");
    assert!(r.is_valid);
}

#[test]
fn set_global_blocked() {
    let r = sanitize("SET GLOBAL max_connections = 100");
    assert!(!r.is_valid);
}

#[test]
fn comment_removal() {
    let r = sanitize("SELECT * FROM users -- this is a comment");
    assert!(r.is_valid);
    assert_eq!(r.sanitized_query, "SELECT * FROM users");
}

#[test]
fn apply_limit_adds_or_preserves() {
    assert_eq!(
        apply_limit("SELECT * FROM users", 1000),
        "SELECT * FROM users LIMIT 1000"
    );
    assert_eq!(
        apply_limit("SELECT * FROM users LIMIT 10", 1000),
        "SELECT * FROM users LIMIT 10"
    );
    assert_eq!(apply_limit("SHOW TABLES", 1000), "SHOW TABLES");
}

#[test]
fn with_cte_allowed() {
    let r = sanitize("WITH cte AS (SELECT 1) SELECT * FROM cte");
    assert!(r.is_valid);
}

#[test]
fn empty_query() {
    let r = sanitize("");
    assert!(!r.is_valid);
}

#[test]
fn comment_inside_string_preserved() {
    let r = sanitize("SELECT * FROM users WHERE name = '-- not a comment'");
    assert!(r.is_valid);
    assert!(r.sanitized_query.contains("-- not a comment"));
}

#[test]
fn hash_comment_inside_string_preserved() {
    let r = sanitize("SELECT * FROM users WHERE tag = '# hashtag'");
    assert!(r.is_valid);
    assert!(r.sanitized_query.contains("# hashtag"));
}

#[test]
fn block_comment_inside_string_preserved() {
    let r = sanitize("SELECT * FROM users WHERE bio = '/* comment */'");
    assert!(r.is_valid);
    assert!(r.sanitized_query.contains("/* comment */"));
}

#[test]
fn embedded_delete_blocked() {
    let r = sanitize("SELECT * FROM (DELETE FROM users) AS t");
    assert!(!r.is_valid);
}

#[test]
fn embedded_drop_blocked() {
    let r = sanitize("SELECT * FROM users WHERE 1=1 UNION SELECT DROP TABLE users");
    assert!(!r.is_valid);
}

#[test]
fn embedded_insert_blocked() {
    let r = sanitize("SELECT * FROM users; INSERT INTO users VALUES (1)");
    assert!(!r.is_valid);
}

#[test]
fn dml_keyword_in_string_allowed() {
    let r = sanitize("SELECT * FROM users WHERE action = 'DELETE'");
    assert!(r.is_valid);
}

#[test]
fn drop_keyword_in_string_allowed() {
    let r = sanitize("SELECT * FROM logs WHERE message = 'DROP TABLE executed'");
    assert!(r.is_valid);
}

#[test]
fn update_keyword_in_string_allowed() {
    let r = sanitize("SELECT * FROM events WHERE type = 'UPDATE'");
    assert!(r.is_valid);
}

#[test]
fn backtick_identifiers() {
    let r = sanitize("SELECT `select`, `from`, `where` FROM `my-table`");
    assert!(r.is_valid);
}

#[test]
fn unicode_in_query() {
    let r = sanitize("SELECT * FROM users WHERE name = 'Rene'");
    assert!(r.is_valid);
}

#[test]
fn very_long_query() {
    let long_cols = (0..200)
        .map(|i| format!("col_{i}"))
        .collect::<Vec<_>>()
        .join(", ");
    let q = format!("SELECT {long_cols} FROM big_table");
    let r = sanitize(&q);
    assert!(r.is_valid);
}

#[test]
fn escaped_quote_in_string() {
    let r = sanitize(r"SELECT * FROM users WHERE name = 'O\'Brien'");
    assert!(r.is_valid);
}

#[test]
fn comment_bypass_attempt() {
    let r = sanitize("SELECT 1 -- \nDROP TABLE users");
    assert!(!r.is_valid);
}

#[test]
fn block_comment_bypass_attempt() {
    let r = sanitize("SELECT 1 /* */ DROP TABLE users");
    assert!(!r.is_valid);
}

#[test]
fn truncate_blocked_anywhere() {
    let r = sanitize("SELECT 1 UNION ALL SELECT TRUNCATE(1.5, 0)");
    assert!(!r.is_valid);
}

#[test]
fn explain_allowed() {
    let r = sanitize("EXPLAIN SELECT * FROM users WHERE id = 1");
    assert!(r.is_valid);
}

#[test]
fn into_dumpfile_blocked() {
    let r = sanitize("SELECT * FROM users INTO DUMPFILE '/tmp/dump'");
    assert!(!r.is_valid);
}

#[test]
fn lock_in_share_mode_blocked() {
    let r = sanitize("SELECT * FROM users LOCK IN SHARE MODE");
    assert!(!r.is_valid);
}
