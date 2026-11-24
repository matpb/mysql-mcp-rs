-- Test-only fixture for docker-compose.live.yml. Passwords are throwaway, not secrets.
CREATE DATABASE IF NOT EXISTS app;
CREATE TABLE IF NOT EXISTS app.widgets (id INT PRIMARY KEY, name VARCHAR(50));
INSERT INTO app.widgets (id, name) VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma');

CREATE USER IF NOT EXISTS 'mcp_full'@'%' IDENTIFIED BY 'live-test-only';
CREATE USER IF NOT EXISTS 'mcp_min'@'%' IDENTIFIED BY 'live-test-only';

GRANT SELECT ON app.* TO 'mcp_full'@'%';
GRANT SELECT ON performance_schema.* TO 'mcp_full'@'%';
GRANT SELECT ON mysql.general_log TO 'mcp_full'@'%';
GRANT SELECT ON mysql.slow_log TO 'mcp_full'@'%';

GRANT SELECT ON app.* TO 'mcp_min'@'%';

FLUSH PRIVILEGES;
