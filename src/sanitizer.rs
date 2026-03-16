use regex::Regex;
use std::sync::LazyLock;

pub struct SanitizeResult {
    pub is_valid: bool,
    pub error: Option<String>,
    pub sanitized_query: String,
}

static MUTATION_PATTERNS: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    [
        r"(?i)^\s*INSERT\s+",
        r"(?i)^\s*UPDATE\s+",
        r"(?i)^\s*DELETE\s+",
        r"(?i)^\s*DROP\s+",
        r"(?i)^\s*CREATE\s+",
        r"(?i)^\s*ALTER\s+",
        r"(?i)^\s*TRUNCATE\s+",
        r"(?i)^\s*RENAME\s+",
        r"(?i)^\s*REPLACE\s+",
        r"(?i)^\s*LOAD\s+",
        r"(?i)^\s*GRANT\s+",
        r"(?i)^\s*REVOKE\s+",
        r"(?i)^\s*FLUSH\s+",
        r"(?i)^\s*LOCK\s+",
        r"(?i)^\s*UNLOCK\s+",
        r"(?i)^\s*CALL\s+",
        r"(?i)^\s*START\s+TRANSACTION",
        r"(?i)^\s*BEGIN",
        r"(?i)^\s*COMMIT",
        r"(?i)^\s*ROLLBACK",
        r"(?i)^\s*SAVEPOINT",
        r"(?i)^\s*RELEASE\s+SAVEPOINT",
    ]
    .iter()
    .map(|p| Regex::new(p).unwrap())
    .collect()
});

static ALLOWED_PATTERNS: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    [
        r"(?i)^\s*SELECT\s+",
        r"(?i)^\s*SHOW\s+",
        r"(?i)^\s*DESCRIBE\s+",
        r"(?i)^\s*DESC\s+",
        r"(?i)^\s*EXPLAIN\s+",
        r"(?i)^\s*WITH\s+",
        r"(?i)^\s*SET\s+@",
    ]
    .iter()
    .map(|p| Regex::new(p).unwrap())
    .collect()
});

static DANGEROUS_KEYWORDS: &[&str] = &[
    "INTO OUTFILE",
    "INTO DUMPFILE",
    "FOR UPDATE",
    "LOCK IN SHARE MODE",
];

static EMBEDDED_DML: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    [
        r"(?i)\bINSERT\b",
        r"(?i)\bUPDATE\b",
        r"(?i)\bDELETE\b",
        r"(?i)\bDROP\b",
        r"(?i)\bCREATE\b",
        r"(?i)\bALTER\b",
        r"(?i)\bTRUNCATE\b",
        r"(?i)\bRENAME\b",
        r"(?i)\bREPLACE\b",
        r"(?i)\bLOAD\b",
        r"(?i)\bGRANT\b",
        r"(?i)\bREVOKE\b",
    ]
    .iter()
    .map(|p| Regex::new(p).unwrap())
    .collect()
});

static HAS_LIMIT: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?i)\bLIMIT\s+\d+").unwrap());
static IS_SELECT: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?i)^\s*SELECT\s+").unwrap());

pub fn sanitize(query: &str) -> SanitizeResult {
    let sanitized = remove_comments(query).trim().to_string();

    if sanitized.is_empty() {
        return SanitizeResult {
            is_valid: false,
            error: Some("Query is empty".into()),
            sanitized_query: String::new(),
        };
    }

    // Check for mutation patterns
    for pattern in MUTATION_PATTERNS.iter() {
        if pattern.is_match(&sanitized) {
            return SanitizeResult {
                is_valid: false,
                error: Some(format!("Query contains mutation operation: {}", pattern.as_str())),
                sanitized_query: String::new(),
            };
        }
    }

    // Check if query starts with allowed pattern
    let starts_with_allowed = ALLOWED_PATTERNS.iter().any(|p| p.is_match(&sanitized));
    if !starts_with_allowed {
        return SanitizeResult {
            is_valid: false,
            error: Some(
                "Query must start with SELECT, SHOW, DESCRIBE, DESC, EXPLAIN, WITH, or SET @"
                    .into(),
            ),
            sanitized_query: String::new(),
        };
    }

    // Check for dangerous keywords
    let upper = sanitized.to_uppercase();
    for keyword in DANGEROUS_KEYWORDS {
        if upper.contains(keyword) {
            return SanitizeResult {
                is_valid: false,
                error: Some(format!("Query contains dangerous keyword: {keyword}")),
                sanitized_query: String::new(),
            };
        }
    }

    // Defense-in-depth: scan for DML keywords anywhere in the query (outside string literals)
    let stripped = strip_string_literals(&sanitized);
    for pattern in EMBEDDED_DML.iter() {
        if pattern.is_match(&stripped) {
            return SanitizeResult {
                is_valid: false,
                error: Some(format!(
                    "Query contains forbidden keyword: {}",
                    pattern.as_str()
                )),
                sanitized_query: String::new(),
            };
        }
    }

    // Check for multiple statements
    if has_multiple_statements(&sanitized) {
        return SanitizeResult {
            is_valid: false,
            error: Some("Multiple statements are not allowed".into()),
            sanitized_query: String::new(),
        };
    }

    SanitizeResult {
        is_valid: true,
        error: None,
        sanitized_query: sanitized,
    }
}

/// Apply a LIMIT clause to SELECT queries that don't have one.
pub fn apply_limit(query: &str, max_rows: u32) -> String {
    if IS_SELECT.is_match(query) && !HAS_LIMIT.is_match(query) {
        format!("{query} LIMIT {max_rows}")
    } else {
        query.to_string()
    }
}

fn remove_comments(query: &str) -> String {
    let mut result = String::with_capacity(query.len());
    let chars: Vec<char> = query.chars().collect();
    let len = chars.len();
    let mut i = 0;
    let mut in_string = false;
    let mut string_char = ' ';

    while i < len {
        // Track string literals — don't strip comments inside them
        if !in_string && (chars[i] == '\'' || chars[i] == '"') {
            in_string = true;
            string_char = chars[i];
            result.push(chars[i]);
            i += 1;
            continue;
        }
        if in_string {
            if chars[i] == '\\' && i + 1 < len {
                // Escaped character — push both and skip
                result.push(chars[i]);
                result.push(chars[i + 1]);
                i += 2;
                continue;
            }
            if chars[i] == string_char {
                in_string = false;
            }
            result.push(chars[i]);
            i += 1;
            continue;
        }

        // -- line comment
        if i + 1 < len && chars[i] == '-' && chars[i + 1] == '-' {
            while i < len && chars[i] != '\n' {
                i += 1;
            }
            continue;
        }
        // # line comment (MySQL specific)
        if chars[i] == '#' {
            while i < len && chars[i] != '\n' {
                i += 1;
            }
            continue;
        }
        // /* block comment */
        if i + 1 < len && chars[i] == '/' && chars[i + 1] == '*' {
            i += 2;
            while i + 1 < len && !(chars[i] == '*' && chars[i + 1] == '/') {
                i += 1;
            }
            if i + 1 < len {
                i += 2; // skip */
            }
            continue;
        }
        result.push(chars[i]);
        i += 1;
    }

    result
}

/// Remove the contents of string literals, leaving empty quotes, so that
/// keyword scanning doesn't match text inside user-provided strings.
fn strip_string_literals(query: &str) -> String {
    let mut result = String::with_capacity(query.len());
    let chars: Vec<char> = query.chars().collect();
    let len = chars.len();
    let mut i = 0;

    while i < len {
        if chars[i] == '\'' || chars[i] == '"' {
            let quote = chars[i];
            result.push(quote);
            i += 1;
            while i < len {
                if chars[i] == '\\' && i + 1 < len {
                    i += 2; // skip escaped char
                    continue;
                }
                if chars[i] == quote {
                    break;
                }
                i += 1;
            }
            if i < len {
                result.push(quote); // closing quote
                i += 1;
            }
        } else {
            result.push(chars[i]);
            i += 1;
        }
    }

    result
}

fn has_multiple_statements(query: &str) -> bool {
    let mut in_string = false;
    let mut string_char = ' ';
    let mut escaped = false;

    let chars: Vec<char> = query.chars().collect();
    let len = chars.len();

    for i in 0..len {
        let ch = chars[i];

        if escaped {
            escaped = false;
            continue;
        }

        if ch == '\\' {
            escaped = true;
            continue;
        }

        if !in_string && (ch == '"' || ch == '\'') {
            in_string = true;
            string_char = ch;
        } else if in_string && ch == string_char {
            in_string = false;
        } else if !in_string && ch == ';' {
            // Check if there's content after the semicolon
            let remaining = query[i + 1..].trim();
            if !remaining.is_empty() {
                return true;
            }
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select_allowed() {
        let r = sanitize("SELECT * FROM users");
        assert!(r.is_valid);
    }

    #[test]
    fn test_insert_blocked() {
        let r = sanitize("INSERT INTO users VALUES (1, 'test')");
        assert!(!r.is_valid);
    }

    #[test]
    fn test_drop_blocked() {
        let r = sanitize("DROP TABLE users");
        assert!(!r.is_valid);
    }

    #[test]
    fn test_show_allowed() {
        let r = sanitize("SHOW TABLES");
        assert!(r.is_valid);
    }

    #[test]
    fn test_describe_allowed() {
        let r = sanitize("DESCRIBE users");
        assert!(r.is_valid);
    }

    #[test]
    fn test_multi_statement_blocked() {
        let r = sanitize("SELECT 1; DROP TABLE users");
        assert!(!r.is_valid);
    }

    #[test]
    fn test_trailing_semicolon_ok() {
        let r = sanitize("SELECT 1;");
        assert!(r.is_valid);
    }

    #[test]
    fn test_into_outfile_blocked() {
        let r = sanitize("SELECT * FROM users INTO OUTFILE '/tmp/data'");
        assert!(!r.is_valid);
    }

    #[test]
    fn test_for_update_blocked() {
        let r = sanitize("SELECT * FROM users FOR UPDATE");
        assert!(!r.is_valid);
    }

    #[test]
    fn test_set_session_var_allowed() {
        let r = sanitize("SET @foo = 1");
        assert!(r.is_valid);
    }

    #[test]
    fn test_set_global_blocked() {
        let r = sanitize("SET GLOBAL max_connections = 100");
        assert!(!r.is_valid);
    }

    #[test]
    fn test_comment_removal() {
        let r = sanitize("SELECT * FROM users -- this is a comment");
        assert!(r.is_valid);
        assert_eq!(r.sanitized_query, "SELECT * FROM users");
    }

    #[test]
    fn test_apply_limit() {
        assert_eq!(apply_limit("SELECT * FROM users", 1000), "SELECT * FROM users LIMIT 1000");
        assert_eq!(apply_limit("SELECT * FROM users LIMIT 10", 1000), "SELECT * FROM users LIMIT 10");
        assert_eq!(apply_limit("SHOW TABLES", 1000), "SHOW TABLES");
    }

    #[test]
    fn test_with_cte_allowed() {
        let r = sanitize("WITH cte AS (SELECT 1) SELECT * FROM cte");
        assert!(r.is_valid);
    }

    #[test]
    fn test_empty_query() {
        let r = sanitize("");
        assert!(!r.is_valid);
    }

    // --- Additional security test cases ---

    #[test]
    fn test_comment_inside_string_preserved() {
        let r = sanitize("SELECT * FROM users WHERE name = '-- not a comment'");
        assert!(r.is_valid);
        assert!(r.sanitized_query.contains("-- not a comment"));
    }

    #[test]
    fn test_hash_comment_inside_string_preserved() {
        let r = sanitize("SELECT * FROM users WHERE tag = '# hashtag'");
        assert!(r.is_valid);
        assert!(r.sanitized_query.contains("# hashtag"));
    }

    #[test]
    fn test_block_comment_inside_string_preserved() {
        let r = sanitize("SELECT * FROM users WHERE bio = '/* comment */'");
        assert!(r.is_valid);
        assert!(r.sanitized_query.contains("/* comment */"));
    }

    #[test]
    fn test_embedded_delete_blocked() {
        let r = sanitize("SELECT * FROM (DELETE FROM users) AS t");
        assert!(!r.is_valid);
    }

    #[test]
    fn test_embedded_drop_blocked() {
        let r = sanitize("SELECT * FROM users WHERE 1=1 UNION SELECT DROP TABLE users");
        assert!(!r.is_valid);
    }

    #[test]
    fn test_embedded_insert_blocked() {
        let r = sanitize("SELECT * FROM users; INSERT INTO users VALUES (1)");
        assert!(!r.is_valid);
    }

    #[test]
    fn test_dml_keyword_in_string_allowed() {
        let r = sanitize("SELECT * FROM users WHERE action = 'DELETE'");
        assert!(r.is_valid);
    }

    #[test]
    fn test_drop_keyword_in_string_allowed() {
        let r = sanitize("SELECT * FROM logs WHERE message = 'DROP TABLE executed'");
        assert!(r.is_valid);
    }

    #[test]
    fn test_update_keyword_in_string_allowed() {
        let r = sanitize("SELECT * FROM events WHERE type = 'UPDATE'");
        assert!(r.is_valid);
    }

    #[test]
    fn test_backtick_identifiers() {
        let r = sanitize("SELECT `select`, `from`, `where` FROM `my-table`");
        assert!(r.is_valid);
    }

    #[test]
    fn test_unicode_in_query() {
        let r = sanitize("SELECT * FROM users WHERE name = 'Rene'");
        assert!(r.is_valid);
    }

    #[test]
    fn test_very_long_query() {
        let long_cols = (0..200).map(|i| format!("col_{i}")).collect::<Vec<_>>().join(", ");
        let q = format!("SELECT {long_cols} FROM big_table");
        let r = sanitize(&q);
        assert!(r.is_valid);
    }

    #[test]
    fn test_escaped_quote_in_string() {
        let r = sanitize(r"SELECT * FROM users WHERE name = 'O\'Brien'");
        assert!(r.is_valid);
    }

    #[test]
    fn test_comment_bypass_attempt() {
        // Attempt to hide DROP after a comment that should be stripped
        let r = sanitize("SELECT 1 -- \nDROP TABLE users");
        assert!(!r.is_valid);
    }

    #[test]
    fn test_block_comment_bypass_attempt() {
        let r = sanitize("SELECT 1 /* */ DROP TABLE users");
        assert!(!r.is_valid);
    }

    #[test]
    fn test_truncate_blocked_anywhere() {
        let r = sanitize("SELECT 1 UNION ALL SELECT TRUNCATE(1.5, 0)");
        // TRUNCATE as a keyword is blocked — the MySQL function TRUNCATE() gets caught too.
        // This is a conservative trade-off: blocking the keyword everywhere prevents abuse.
        assert!(!r.is_valid);
    }

    #[test]
    fn test_explain_allowed() {
        let r = sanitize("EXPLAIN SELECT * FROM users WHERE id = 1");
        assert!(r.is_valid);
    }

    #[test]
    fn test_into_dumpfile_blocked() {
        let r = sanitize("SELECT * FROM users INTO DUMPFILE '/tmp/dump'");
        assert!(!r.is_valid);
    }

    #[test]
    fn test_lock_in_share_mode_blocked() {
        let r = sanitize("SELECT * FROM users LOCK IN SHARE MODE");
        assert!(!r.is_valid);
    }

    #[test]
    fn test_strip_string_literals() {
        assert_eq!(strip_string_literals("SELECT 'DROP TABLE'"), "SELECT ''");
        assert_eq!(strip_string_literals(r"SELECT 'it\'s'"), "SELECT ''");
        assert_eq!(strip_string_literals("SELECT \"DELETE\""), "SELECT \"\"");
    }
}
