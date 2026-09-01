//! Read-only query gate. This is a usability guard that catches obvious writes early;
//! the actual boundary is the MySQL account's read-only GRANT.

use regex::Regex;
use std::sync::LazyLock;

pub struct SanitizeResult {
    pub is_valid: bool,
    pub error: Option<String>,
    pub sanitized_query: String,
}

static MUTATION_PREFIX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)^\s*(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|RENAME|REPLACE|LOAD|GRANT|REVOKE|FLUSH|LOCK|UNLOCK|CALL|START\s+TRANSACTION|BEGIN|COMMIT|ROLLBACK|SAVEPOINT|RELEASE\s+SAVEPOINT)\b",
    )
    .unwrap()
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
        r"(?i)^\s*TABLE\s+",
        r"(?i)^\s*VALUES\s+",
        r"(?i)^\s*\(\s*(?:\(\s*)*(?:SELECT|TABLE|VALUES|WITH)\b",
    ]
    .iter()
    .map(|p| Regex::new(p).unwrap())
    .collect()
});

static SET_USER_VAR: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?i)^\s*SET\s+@").unwrap());
static SET_SCOPE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:GLOBAL|PERSIST_ONLY|PERSIST|SESSION|LOCAL)\b").unwrap());

/// `SET @@x = 1` is a session write that names no scope keyword.
static SET_SYSTEM_ASSIGN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"@@[A-Za-z0-9_.$]*\s*=").unwrap());

/// A `@@var` reference, so the scope scan cannot fire on the read side of `SET @x = @@session.y`.
static SYSTEM_VAR_REF: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"@@[A-Za-z0-9_.$]*").unwrap());

static DANGEROUS_CONSTRUCTS: LazyLock<Vec<(&'static str, Regex)>> = LazyLock::new(|| {
    [
        ("INTO OUTFILE", r"(?i)\bINTO\s+OUTFILE\b"),
        ("INTO DUMPFILE", r"(?i)\bINTO\s+DUMPFILE\b"),
        ("FOR UPDATE", r"(?i)\bFOR\s+UPDATE\b"),
        ("FOR SHARE", r"(?i)\bFOR\s+SHARE\b"),
        ("LOCK IN SHARE MODE", r"(?i)\bLOCK\s+IN\s+SHARE\s+MODE\b"),
        ("LOAD_FILE()", r"(?i)\bLOAD_FILE\s*\("),
    ]
    .iter()
    .map(|(label, p)| (*label, Regex::new(p).unwrap()))
    .collect()
});

// Reserved verbs cannot be unquoted identifiers, so `VERB` + whitespace is enough; the
// non-reserved ones are also function names, so they need their statement object token.
static EMBEDDED_WRITE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)\b(?:INSERT|UPDATE|DELETE|DROP|ALTER|RENAME|GRANT|REVOKE)\s|\bREPLACE\s+(?:LOW_PRIORITY\s+|DELAYED\s+)*INTO\b|\bTRUNCATE\s+TABLE\b|\bLOAD\s+(?:DATA|XML)\b",
    )
    .unwrap()
});

// Leftmost-first: the SHOW CREATE alternative consumes the CREATE token, so only a bare
// CREATE elsewhere makes group 1 participate.
static EMBEDDED_CREATE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\bSHOW\s+CREATE\b|\b(CREATE)\b").unwrap());

static LIMITABLE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)^\s*(?:SELECT\b|WITH\b|TABLE\b|\(\s*(?:\(\s*)*(?:SELECT|TABLE|WITH)\b)")
        .unwrap()
});
static LIMIT_KEYWORD: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?i)\bLIMIT\b").unwrap());
static TRAILING_LIMIT: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\bLIMIT\s+(\d+)\s*(?:,\s*(\d+)\s*)?(?:OFFSET\s+\d+\s*)?;?\s*$").unwrap()
});

pub fn sanitize(query: &str) -> SanitizeResult {
    let sanitized = match remove_comments(query) {
        Ok(s) => s.trim().to_string(),
        Err(e) => return reject(e),
    };

    if sanitized.is_empty() {
        return reject("Query is empty".into());
    }

    let mask = mask_quoted(&sanitized);

    if let Some(caps) = MUTATION_PREFIX.captures(&mask) {
        let verb = caps.get(1).map(|m| m.as_str()).unwrap_or("that statement");
        return reject(format!(
            "Query starts with {verb}, which modifies data or session state. This server is read-only. Allowed: SELECT, WITH, TABLE, VALUES, SHOW, DESCRIBE, EXPLAIN, SET @var."
        ));
    }

    if !ALLOWED_PATTERNS.iter().any(|p| p.is_match(&mask)) {
        return reject(
            "Query must start with SELECT, WITH, TABLE, VALUES, SHOW, DESCRIBE, DESC, EXPLAIN, SET @var, or a parenthesized SELECT/TABLE/VALUES/WITH."
                .into(),
        );
    }

    let set_writes_system = SET_SYSTEM_ASSIGN.is_match(&mask)
        || SET_SCOPE.is_match(&SYSTEM_VAR_REF.replace_all(&mask, " "));
    if SET_USER_VAR.is_match(&mask) && set_writes_system {
        return reject(
            "SET may only assign user variables (SET @var = ...). GLOBAL, SESSION and PERSIST scopes change server state and are not allowed."
                .into(),
        );
    }

    for (label, pattern) in DANGEROUS_CONSTRUCTS.iter() {
        if pattern.is_match(&mask) {
            return reject(format!(
                "Query uses {label}, which locks rows or reads/writes server files. Remove it; plain SELECT is allowed, and SHOW CREATE TABLE gives schema."
            ));
        }
    }

    let embedded = EMBEDDED_WRITE
        .find(&mask)
        .map(|m| m.as_str().trim().to_string())
        .or_else(|| {
            EMBEDDED_CREATE
                .captures_iter(&mask)
                .find_map(|c| c.get(1).map(|m| m.as_str().to_string()))
        });
    if let Some(construct) = embedded {
        return reject(format!(
            "Query contains the write statement '{construct}'. This server is read-only. REPLACE(), INSERT() and TRUNCATE() as functions are allowed — only the statement forms are blocked."
        ));
    }

    if has_multiple_statements(&mask) {
        return reject(
            "Multiple statements are not allowed. Send one statement per call (a single trailing ';' is fine)."
                .into(),
        );
    }

    SanitizeResult {
        is_valid: true,
        error: None,
        sanitized_query: sanitized,
    }
}

fn reject(error: String) -> SanitizeResult {
    SanitizeResult {
        is_valid: false,
        error: Some(error),
        sanitized_query: String::new(),
    }
}

/// Bound row-returning queries: append a LIMIT, or clamp one the caller supplied.
pub fn apply_limit(query: &str, max_rows: u32) -> String {
    let mask = mask_quoted(query);

    if !LIMITABLE.is_match(&mask) {
        return query.to_string();
    }

    let top_level_limit = LIMIT_KEYWORD
        .find_iter(&mask)
        .map(|m| m.start())
        .filter(|start| {
            let prefix = &mask[..*start];
            prefix.matches('(').count() == prefix.matches(')').count()
        })
        .last();

    let Some(start) = top_level_limit else {
        let trimmed = query.trim_end().trim_end_matches(';').trim_end();
        return format!("{trimmed} LIMIT {max_rows}");
    };

    // A non-numeric trailing LIMIT (`LIMIT ?`, `LIMIT @n`) is left alone: appending a
    // second LIMIT would be a guaranteed syntax error.
    let Some(caps) = TRAILING_LIMIT.captures(&mask[start..]) else {
        return query.to_string();
    };

    let row_count = caps.get(2).or_else(|| caps.get(1)).unwrap();
    if row_count.as_str().parse::<u64>().unwrap_or(u64::MAX) <= u64::from(max_rows) {
        return query.to_string();
    }

    let from = start + row_count.start();
    let to = start + row_count.end();
    format!("{}{}{}", &query[..from], max_rows, &query[to..])
}

fn remove_comments(query: &str) -> Result<String, String> {
    let mut result = String::with_capacity(query.len());
    let chars: Vec<char> = query.chars().collect();
    let len = chars.len();
    let mut i = 0;
    let mut in_string = false;
    let mut string_char = ' ';

    while i < len {
        // Track quoted runs — don't strip comments inside them
        if !in_string && (chars[i] == '\'' || chars[i] == '"' || chars[i] == '`') {
            in_string = true;
            string_char = chars[i];
            result.push(chars[i]);
            i += 1;
            continue;
        }
        if in_string {
            // Backslash is a literal character inside a backticked identifier.
            if string_char != '`' && chars[i] == '\\' && i + 1 < len {
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

        // MySQL starts a -- comment only when followed by whitespace; `1--2` is arithmetic.
        if i + 1 < len
            && chars[i] == '-'
            && chars[i + 1] == '-'
            && (i + 2 >= len || chars[i + 2].is_whitespace() || chars[i + 2].is_control())
        {
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
        if i + 1 < len && chars[i] == '/' && chars[i + 1] == '*' {
            match chars.get(i + 2) {
                // MySQL executes the body of a version-gated comment, so stripping it would
                // change what the server runs.
                Some('!') => {
                    return Err("MySQL version-gated comments (/*! ... */) are not supported — their contents are executed by the server. Remove the comment and write the statement plainly. Optimizer hints (/*+ ... */) are allowed.".into());
                }
                Some('+') => {
                    result.push(chars[i]);
                    result.push(chars[i + 1]);
                    i += 2;
                    while i + 1 < len && !(chars[i] == '*' && chars[i + 1] == '/') {
                        result.push(chars[i]);
                        i += 1;
                    }
                    if i + 1 < len {
                        result.push(chars[i]);
                        result.push(chars[i + 1]);
                        i += 2;
                    } else {
                        while i < len {
                            result.push(chars[i]);
                            i += 1;
                        }
                    }
                    continue;
                }
                _ => {
                    i += 2;
                    while i + 1 < len && !(chars[i] == '*' && chars[i + 1] == '/') {
                        i += 1;
                    }
                    i = if i + 1 < len { i + 2 } else { len };
                    // A comment separates tokens: `x/**/FROM` must not fuse into `xFROM`.
                    result.push(' ');
                    continue;
                }
            }
        }
        result.push(chars[i]);
        i += 1;
    }

    Ok(result)
}

/// Blank the interior of every quoted run, keeping the delimiters and the byte length,
/// so offsets found in the mask index the original query.
fn mask_quoted(query: &str) -> String {
    let mut result = String::with_capacity(query.len());
    let chars: Vec<char> = query.chars().collect();
    let len = chars.len();
    let mut i = 0;

    while i < len {
        let delimiter = chars[i];
        if delimiter != '\'' && delimiter != '"' && delimiter != '`' {
            result.push(delimiter);
            i += 1;
            continue;
        }

        result.push(delimiter);
        i += 1;
        while i < len {
            if delimiter != '`' && chars[i] == '\\' && i + 1 < len {
                result.push(' ');
                result.push_str(&" ".repeat(chars[i + 1].len_utf8()));
                i += 2;
                continue;
            }
            if chars[i] == delimiter {
                break;
            }
            result.push_str(&" ".repeat(chars[i].len_utf8()));
            i += 1;
        }
        if i < len {
            result.push(delimiter);
            i += 1;
        }
    }

    result
}

fn has_multiple_statements(mask: &str) -> bool {
    mask.match_indices(';')
        .any(|(i, _)| !mask[i + 1..].trim().is_empty())
}

#[cfg(test)]
mod tests {
    use super::{apply_limit, mask_quoted};

    #[test]
    fn mask_quoted_redacts_quoted_content() {
        assert_eq!(mask_quoted("SELECT 'DROP TABLE'"), "SELECT '          '");
        assert_eq!(mask_quoted("SELECT \"DELETE\""), "SELECT \"      \"");
        assert_eq!(mask_quoted("SELECT `a;b`"), "SELECT `   `");
        assert_eq!(mask_quoted(r"SELECT 'it\'s'"), "SELECT '     '");
    }

    #[test]
    fn mask_quoted_preserves_byte_length() {
        // The LIMIT rewrite splices by byte offset found in the mask.
        let q = "SELECT * FROM t WHERE n = 'René' AND e = '🙂' LIMIT 5000000";
        assert_eq!(mask_quoted(q).len(), q.len());
    }

    #[test]
    fn apply_limit_appends_to_bare_select() {
        assert_eq!(
            apply_limit("SELECT * FROM t", 1000),
            "SELECT * FROM t LIMIT 1000"
        );
    }

    #[test]
    fn apply_limit_trims_trailing_semicolon() {
        // The bug this guards: `... ; LIMIT n` is a MySQL syntax error (1064).
        assert_eq!(
            apply_limit("SELECT * FROM t;", 1000),
            "SELECT * FROM t LIMIT 1000"
        );
        assert_eq!(
            apply_limit("SELECT id FROM t ORDER BY id ;", 1000),
            "SELECT id FROM t ORDER BY id LIMIT 1000"
        );
    }

    #[test]
    fn apply_limit_leaves_existing_limit_untouched() {
        // Already limited below the cap: returned verbatim, trailing semicolon and all.
        assert_eq!(
            apply_limit("SELECT * FROM t LIMIT 10;", 1000),
            "SELECT * FROM t LIMIT 10;"
        );
    }

    #[test]
    fn apply_limit_ignores_non_select() {
        assert_eq!(apply_limit("SHOW TABLES;", 1000), "SHOW TABLES;");
    }
}
