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
fn truncate_function_allowed() {
    // TRUNCATE(x, d) is a numeric function, not the DDL statement.
    let r = sanitize("SELECT 1 UNION ALL SELECT TRUNCATE(1.5, 0)");
    assert!(r.is_valid);
}

#[test]
fn truncate_table_blocked_anywhere() {
    assert!(!sanitize("SELECT 1 /* */ TRUNCATE TABLE users").is_valid);
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

// --- Statement-level writes stay rejected (read-only invariant) ---

#[test]
fn update_statement_blocked() {
    assert!(!sanitize("UPDATE users SET name = 'x'").is_valid);
}

#[test]
fn delete_statement_blocked() {
    assert!(!sanitize("DELETE FROM users").is_valid);
}

#[test]
fn create_table_statement_blocked() {
    assert!(!sanitize("CREATE TABLE t (a INT)").is_valid);
}

#[test]
fn alter_statement_blocked() {
    assert!(!sanitize("ALTER TABLE users ADD COLUMN c INT").is_valid);
}

#[test]
fn truncate_table_statement_blocked() {
    assert!(!sanitize("TRUNCATE TABLE users").is_valid);
}

#[test]
fn replace_into_statement_blocked() {
    assert!(!sanitize("REPLACE INTO users VALUES (1)").is_valid);
}

#[test]
fn grant_statement_blocked() {
    assert!(!sanitize("GRANT ALL ON *.* TO 'u'@'%'").is_valid);
}

#[test]
fn lock_tables_blocked() {
    assert!(!sanitize("LOCK TABLES users READ").is_valid);
}

#[test]
fn call_procedure_blocked() {
    assert!(!sanitize("CALL do_something(1)").is_valid);
}

#[test]
fn multi_statement_with_select_blocked() {
    assert!(!sanitize("SELECT 1; SELECT 2").is_valid);
}

// --- Defect 06: `--` is a comment only when followed by whitespace ---

#[test]
fn double_dash_without_space_is_arithmetic() {
    let r = sanitize("SELECT 1--2 AS weird");
    assert!(r.is_valid);
    assert!(r.sanitized_query.contains("1--2"));
}

#[test]
fn double_dash_negative_literal_preserved() {
    let r = sanitize("SELECT 5--3");
    assert!(r.is_valid);
    assert_eq!(r.sanitized_query, "SELECT 5--3");
}

#[test]
fn double_dash_with_space_still_stripped() {
    assert_eq!(sanitize("SELECT 1 --\tignored").sanitized_query, "SELECT 1");
    assert_eq!(sanitize("SELECT 1 --\n").sanitized_query, "SELECT 1");
}

#[test]
fn double_dash_at_end_of_input_is_comment() {
    let r = sanitize("SELECT 1 --");
    assert!(r.is_valid);
    assert_eq!(r.sanitized_query, "SELECT 1");
}

// --- Defect 06b: backticked identifiers are opaque to comment stripping ---

#[test]
fn backtick_identifier_with_hash() {
    let r = sanitize("SELECT `col#1`, x FROM t");
    assert!(r.is_valid);
    assert!(r.sanitized_query.contains("`col#1`"));
}

#[test]
fn backtick_identifier_with_double_dash() {
    let r = sanitize("SELECT `col-- 1`, x FROM t");
    assert!(r.is_valid);
    assert!(r.sanitized_query.contains("`col-- 1`"));
}

#[test]
fn backtick_identifier_with_block_comment() {
    let r = sanitize("SELECT `a/*b*/c`, x FROM t");
    assert!(r.is_valid);
    assert!(r.sanitized_query.contains("`a/*b*/c`"));
}

#[test]
fn backtick_identifier_with_backslash() {
    let r = sanitize(r"SELECT `a\b` FROM t");
    assert!(r.is_valid);
    assert!(r.sanitized_query.contains(r"`a\b`"));
}

// --- Defect 09/10: the row cap ---

#[test]
fn apply_limit_caps_cte() {
    assert_eq!(
        apply_limit("WITH c AS (SELECT 1) SELECT * FROM c", 1000),
        "WITH c AS (SELECT 1) SELECT * FROM c LIMIT 1000"
    );
}

#[test]
fn apply_limit_caps_cte_with_inner_limit() {
    assert_eq!(
        apply_limit("WITH c AS (SELECT 1 LIMIT 1) SELECT * FROM c", 1000),
        "WITH c AS (SELECT 1 LIMIT 1) SELECT * FROM c LIMIT 1000"
    );
}

#[test]
fn apply_limit_caps_table_statement() {
    assert_eq!(apply_limit("TABLE users", 1000), "TABLE users LIMIT 1000");
}

#[test]
fn apply_limit_caps_parenthesized_union() {
    assert_eq!(
        apply_limit("(SELECT a FROM t1) UNION (SELECT b FROM t2)", 1000),
        "(SELECT a FROM t1) UNION (SELECT b FROM t2) LIMIT 1000"
    );
}

#[test]
fn apply_limit_ignores_explain() {
    assert_eq!(
        apply_limit("EXPLAIN SELECT * FROM t", 1000),
        "EXPLAIN SELECT * FROM t"
    );
}

#[test]
fn apply_limit_ignores_values() {
    assert_eq!(apply_limit("VALUES ROW(1,2)", 1000), "VALUES ROW(1,2)");
}

#[test]
fn apply_limit_ignores_show() {
    assert_eq!(
        apply_limit("SHOW COLUMNS FROM t", 1000),
        "SHOW COLUMNS FROM t"
    );
}

#[test]
fn apply_limit_ignores_subquery_limit() {
    assert_eq!(
        apply_limit("SELECT id, (SELECT x FROM y LIMIT 1) z FROM huge", 1000),
        "SELECT id, (SELECT x FROM y LIMIT 1) z FROM huge LIMIT 1000"
    );
}

#[test]
fn apply_limit_ignores_derived_table_limit() {
    assert_eq!(
        apply_limit(
            "SELECT * FROM (SELECT * FROM a LIMIT 10) q JOIN big b",
            1000
        ),
        "SELECT * FROM (SELECT * FROM a LIMIT 10) q JOIN big b LIMIT 1000"
    );
}

#[test]
fn apply_limit_ignores_limit_in_string() {
    assert_eq!(
        apply_limit("SELECT * FROM t WHERE s = 'LIMIT 5'", 1000),
        "SELECT * FROM t WHERE s = 'LIMIT 5' LIMIT 1000"
    );
}

#[test]
fn apply_limit_caps_oversized_user_limit() {
    assert_eq!(
        apply_limit("SELECT * FROM t LIMIT 5000000", 1000),
        "SELECT * FROM t LIMIT 1000"
    );
}

#[test]
fn apply_limit_caps_offset_comma_form() {
    assert_eq!(
        apply_limit("SELECT * FROM t LIMIT 100, 5000000", 1000),
        "SELECT * FROM t LIMIT 100, 1000"
    );
}

#[test]
fn apply_limit_caps_offset_keyword_form() {
    assert_eq!(
        apply_limit("SELECT * FROM t LIMIT 5000000 OFFSET 20", 1000),
        "SELECT * FROM t LIMIT 1000 OFFSET 20"
    );
}

#[test]
fn apply_limit_leaves_small_user_limit() {
    assert_eq!(
        apply_limit("SELECT * FROM t LIMIT 10", 1000),
        "SELECT * FROM t LIMIT 10"
    );
}

#[test]
fn apply_limit_leaves_parameterized_limit() {
    assert_eq!(
        apply_limit("SELECT * FROM t LIMIT @n", 1000),
        "SELECT * FROM t LIMIT @n"
    );
}

#[test]
fn apply_limit_caps_after_multibyte_literal() {
    // Guards the byte-offset splice against a multibyte literal desyncing the mask.
    assert_eq!(
        apply_limit("SELECT * FROM t WHERE n = 'René' LIMIT 5000000", 1000),
        "SELECT * FROM t WHERE n = 'René' LIMIT 1000"
    );
}

// --- Defect 12: SHOW CREATE is read-only ---

#[test]
fn show_create_table_allowed() {
    assert!(sanitize("SHOW CREATE TABLE users").is_valid);
}

#[test]
fn show_create_view_allowed() {
    assert!(sanitize("SHOW CREATE VIEW user_view").is_valid);
}

#[test]
fn show_create_database_allowed() {
    assert!(sanitize("SHOW CREATE DATABASE mydb").is_valid);
}

#[test]
fn show_create_procedure_allowed() {
    assert!(sanitize("SHOW CREATE PROCEDURE my_proc").is_valid);
}

#[test]
fn show_create_function_allowed() {
    assert!(sanitize("SHOW CREATE FUNCTION my_func").is_valid);
}

#[test]
fn embedded_create_table_blocked() {
    assert!(!sanitize("SELECT 1 /* */ CREATE TABLE t (a INT)").is_valid);
}

#[test]
fn create_time_column_allowed() {
    assert!(sanitize("SELECT create_time FROM information_schema.tables").is_valid);
}

// --- Defect 13: function forms of REPLACE / TRUNCATE / INSERT ---

#[test]
fn replace_function_allowed() {
    assert!(sanitize("SELECT REPLACE(name,'a','b') FROM t").is_valid);
}

#[test]
fn insert_function_allowed() {
    assert!(sanitize("SELECT INSERT(s,1,2,'x') FROM t").is_valid);
}

#[test]
fn replace_into_blocked() {
    assert!(!sanitize("SELECT 1 /* */ REPLACE INTO t VALUES (1)").is_valid);
}

#[test]
fn load_data_infile_blocked() {
    assert!(!sanitize("SELECT 1 /* */ LOAD DATA INFILE '/tmp/x' INTO TABLE t").is_valid);
}

#[test]
fn update_time_column_allowed() {
    assert!(sanitize("SELECT update_time FROM information_schema.tables").is_valid);
}

// --- Defect 14: read-only statement shapes beyond SELECT ---

#[test]
fn parenthesized_union_allowed() {
    assert!(sanitize("(SELECT a FROM t1) UNION (SELECT b FROM t2)").is_valid);
}

#[test]
fn nested_parenthesized_select_allowed() {
    assert!(sanitize("((SELECT 1)) UNION (SELECT 2)").is_valid);
}

#[test]
fn table_statement_allowed() {
    assert!(sanitize("TABLE users").is_valid);
}

#[test]
fn values_statement_allowed() {
    assert!(sanitize("VALUES ROW(1,2)").is_valid);
}

#[test]
fn parenthesized_insert_rejected() {
    assert!(!sanitize("(INSERT INTO t VALUES (1))").is_valid);
}

// --- Defect 15: dangerous constructs, scanned on the masked query ---

#[test]
fn for_update_inside_string_allowed() {
    assert!(sanitize("SELECT * FROM users WHERE note LIKE '%for update%'").is_valid);
}

#[test]
fn for_update_across_newline_blocked() {
    assert!(!sanitize("SELECT * FROM users\nFOR\n  UPDATE").is_valid);
}

#[test]
fn for_update_backticked_identifier_allowed() {
    assert!(sanitize("SELECT `for update` FROM t").is_valid);
}

#[test]
fn for_share_blocked() {
    assert!(!sanitize("SELECT * FROM users FOR SHARE").is_valid);
}

#[test]
fn load_file_blocked() {
    assert!(!sanitize("SELECT LOAD_FILE('/etc/passwd')").is_valid);
}

// --- Defect 28: rejection messages ---

#[test]
fn rejection_message_has_no_regex_syntax() {
    let r = sanitize("SELECT 1 UNION SELECT DROP TABLE users");
    let msg = r.error.expect("rejected query must carry an error");
    assert!(!msg.contains("(?i)"));
    assert!(!msg.contains(r"\b"));
}

#[test]
fn rejection_message_names_construct() {
    let r = sanitize("SELECT 1 UNION SELECT DROP TABLE users");
    let msg = r.error.expect("rejected query must carry an error");
    assert!(msg.contains("DROP"));
}

// --- Defect 29: version comments vs optimizer hints ---

#[test]
fn version_comment_rejected() {
    assert!(!sanitize("SELECT 1 /*! , 2 */").is_valid);
}

#[test]
fn version_comment_with_number_rejected() {
    assert!(!sanitize("SELECT 1 /*!80000 , 2 */").is_valid);
}

#[test]
fn optimizer_hint_preserved() {
    let r = sanitize("SELECT /*+ MAX_EXECUTION_TIME(1000) */ * FROM t");
    assert!(r.is_valid);
    assert!(
        r.sanitized_query
            .contains("/*+ MAX_EXECUTION_TIME(1000) */")
    );
}

#[test]
fn optimizer_hint_inside_string_not_special() {
    let r = sanitize("SELECT * FROM t WHERE s = '/*! 1 */ /*+ hint */'");
    assert!(r.is_valid);
    assert!(r.sanitized_query.contains("/*! 1 */"));
}

// --- Multi-statement detection now rides on the quote mask ---

#[test]
fn semicolon_inside_backticks_allowed() {
    assert!(sanitize("SELECT `a;b` FROM t").is_valid);
}

#[test]
fn semicolon_inside_string_allowed() {
    assert!(sanitize("SELECT * FROM t WHERE s = 'a;b'").is_valid);
}

// --- SET is limited to user variables ---

#[test]
fn set_user_var_with_global_tail_blocked() {
    assert!(!sanitize("SET @a = 1, GLOBAL general_log = 1").is_valid);
}

#[test]
fn set_session_scope_blocked() {
    assert!(!sanitize("SET SESSION sql_mode = ''").is_valid);
}

#[test]
fn information_schema_global_variables_allowed() {
    assert!(sanitize("SELECT * FROM information_schema.global_variables").is_valid);
}

#[test]
fn version_comment_hiding_multi_statement_rejected() {
    assert!(!sanitize("SELECT 1 /*!32302 ; DROP TABLE t */").is_valid);
}

#[test]
fn set_user_var_with_session_tail_blocked() {
    assert!(!sanitize("SET @a := 1, SESSION sql_mode = ''").is_valid);
}

#[test]
fn optimizer_hint_does_not_block_row_cap() {
    assert_eq!(
        apply_limit("SELECT /*+ MAX_EXECUTION_TIME(1000) */ * FROM t", 1000),
        "SELECT /*+ MAX_EXECUTION_TIME(1000) */ * FROM t LIMIT 1000"
    );
}

#[test]
fn set_bare_system_var_blocked() {
    for q in [
        "SET @@time_zone = '+05:00'",
        "SET @@sql_mode = 'NO_BACKSLASH_ESCAPES'",
        "SET @@max_execution_time = 0",
        "SET @a = 1, @@sql_mode = ''",
    ] {
        assert!(!sanitize(q).is_valid, "should be blocked: {q}");
    }
}

#[test]
fn set_user_var_reading_system_var_allowed() {
    assert!(sanitize("SET @x = @@session.sql_mode").is_valid);
    assert!(sanitize("SET @x = @@global.max_connections").is_valid);
}

#[test]
fn block_comment_separates_tokens() {
    assert_eq!(
        sanitize("SELECT 1 AS x/**/FROM DUAL").sanitized_query,
        "SELECT 1 AS x FROM DUAL"
    );
}
