import { Pool } from "pg";

/**
 * PostgreSQL adapter implementing the same interface as the SQLite adapters.
 * Connects via process.env.DATABASE_URL (e.g. Supabase transaction pooler).
 *
 * SQLite uses `?` placeholders; Postgres uses `$1, $2, ...`.
 * This adapter translates `?` → `$n` automatically.
 */

// Translate `?` placeholders to `$1, $2, ...`, skipping string literals.
function translatePlaceholders(sql) {
  let out = "";
  let idx = 0;
  let i = 0;
  const n = sql.length;
  while (i < n) {
    const ch = sql[i];
    // Single-quoted string literal ('' is an escaped quote)
    if (ch === "'") {
      out += ch;
      i++;
      while (i < n) {
        out += sql[i];
        if (sql[i] === "'") {
          if (sql[i + 1] === "'") {
            out += sql[i + 1];
            i += 2;
            continue;
          }
          i++;
          break;
        }
        i++;
      }
      continue;
    }
    // Double-quoted identifier
    if (ch === '"') {
      out += ch;
      i++;
      while (i < n) {
        out += sql[i];
        if (sql[i] === '"') {
          i++;
          break;
        }
        i++;
      }
      continue;
    }
    // Line comment
    if (ch === "-" && sql[i + 1] === "-") {
      while (i < n && sql[i] !== "\n") {
        out += sql[i];
        i++;
      }
      continue;
    }
    // Block comment
    if (ch === "/" && sql[i + 1] === "*") {
      out += "/*";
      i += 2;
      while (i < n && !(sql[i] === "*" && sql[i + 1] === "/")) {
        out += sql[i];
        i++;
      }
      if (i < n) {
        out += "*/";
        i += 2;
      }
      continue;
    }
    if (ch === "?") {
      idx++;
      out += "$" + idx;
      i++;
      continue;
    }
    out += ch;
    i++;
  }
  return out;
}

// PRAGMA table_info(table) is SQLite-specific. Translate to the Postgres
// information_schema equivalent so migrate.js syncSchemaFromTables() works.
function translatePragmaTableInfo(sql) {
  const m = /^\s*PRAGMA\s+table_info\s*\(\s*["']?([A-Za-z_][A-Za-z0-9_]*)["']?\s*\)\s*;?\s*$/i.exec(sql);
  if (!m) return null;
  const table = m[1].replace(/'/g, "''");
  return `SELECT column_name AS name FROM information_schema.columns WHERE table_name = '${table}'`;
}

// INSERT OR REPLACE is SQLite-specific. Translate to Postgres ON CONFLICT.
// Assumes `id` is the primary key (true for all 9router tables using this).
function translateInsertOrReplace(sql) {
  const m = /^\s*INSERT\s+OR\s+REPLACE\s+INTO\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(([^)]+)\)\s*VALUES/i.exec(sql);
  if (!m) return sql;
  const table = m[1];
  const cols = m[2].split(",").map((c) => c.trim()).filter(Boolean);
  const setClause = cols.map((c) => `${c}=EXCLUDED.${c}`).join(", ");
  return sql.replace(/^\s*INSERT\s+OR\s+REPLACE\s+INTO/i, `INSERT INTO`) + ` ON CONFLICT(id) DO UPDATE SET ${setClause}`;
}

function redactUrl(url) {
  try {
    const u = new URL(url);
    if (u.password) u.password = "***";
    return u.toString();
  } catch {
    return "[unparseable-url]";
  }
}

export async function createPostgresAdapter(connectionString) {
  const pool = new Pool({
    connectionString,
    // Supabase and most hosted Postgres require SSL. rejectUnauthorized:false
    // avoids failures with self-signed or pooled certs.
    ssl: { rejectUnauthorized: false },
    // Keep the pool small — serverless/pooler environments have tight limits.
    max: 5,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 10000,
  });

  // Fail fast if we cannot connect — driver.js falls back to SQLite.
  try {
    await pool.query("SELECT 1");
  } catch (e) {
    try { await pool.end(); } catch {}
    throw new Error(`[DB] postgres connection failed (${redactUrl(connectionString)}): ${e.message}`);
  }

  // Build a query runner bound to either the pool or a transaction client.
  function makeRunner(queryFn) {
    return {
      driver: "postgres",
      async run(sql, params = []) {
        const pragma = translatePragmaTableInfo(sql);
        if (pragma) {
          await queryFn(pragma, []);
          return { changes: 0 };
        }
        const text = translatePlaceholders(translateInsertOrReplace(sql));
        const res = await queryFn(text, params);
        return { changes: res.rowCount ?? 0 };
      },
      async get(sql, params = []) {
        const pragma = translatePragmaTableInfo(sql);
        if (pragma) {
          const res = await queryFn(pragma, []);
          return res.rows[0] ?? undefined;
        }
        const text = translatePlaceholders(translateInsertOrReplace(sql));
        const res = await queryFn(text, params);
        return res.rows[0] ?? undefined;
      },
      async all(sql, params = []) {
        const pragma = translatePragmaTableInfo(sql);
        if (pragma) {
          const res = await queryFn(pragma, []);
          return res.rows;
        }
        const text = translatePlaceholders(translateInsertOrReplace(sql));
        const res = await queryFn(text, params);
        return res.rows;
      },
      async exec(sql) {
        // DDL / multi-statement: pg handles semicolon-separated statements.
        // Skip empty statements to avoid "empty query" errors.
        // Translate SQLite AUTOINCREMENT to Postgres SERIAL.
        const statements = sql.split(";").map((s) => s.trim()).filter(Boolean);
        for (let stmt of statements) {
          stmt = stmt.replace(/INTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT/i, "SERIAL PRIMARY KEY");
          await queryFn(stmt);
        }
      },
      async transaction(fn) {
        const client = await pool.connect();
        try {
          await client.query("BEGIN");
          const tx = makeRunner((text, params) => client.query(text, params || []));
          const result = await fn(tx);
          await client.query("COMMIT");
          return result;
        } catch (e) {
          try { await client.query("ROLLBACK"); } catch {}
          throw e;
        } finally {
          client.release();
        }
      },
      async close() {
        await pool.end();
      },
    };
  }

  const adapter = makeRunner((text, params) => pool.query(text, params || []));

  // Log the redacted connection target (mirrors the sqlite adapter's file log).
  console.log(`[DB] postgres adapter ready (${redactUrl(connectionString)})`);

  return adapter;
}
