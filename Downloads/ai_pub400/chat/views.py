from django.shortcuts import render
from django.http import JsonResponse

import os, re, csv, io, uuid, time
import requests, paramiko


# ==========================================================
# CONFIG (keep credentials in env, not in code)
# ==========================================================

IBMI = {
    "host": os.getenv("IBMI_HOST", "pub400.com"),
    "port": int(os.getenv("IBMI_PORT", "2222")),
    "user": os.getenv("IBMI_USER", "RAJ2001"),
    "password": os.getenv("IBMI_PASSWORD", "Raj@925"),

    "library": os.getenv("IBMI_LIBRARY", "RAJ20011"),
}

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://127.0.0.1:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:7b-instruct")

SQL_BLOCK_RE = re.compile(r"```(?:sql)?\s*(.*?)```", re.S | re.I)

# schema cache (speed)
_SCHEMA_CACHE = {}            # {LIB: {"ts": float, "cols_map": dict}}
SCHEMA_TTL_SECONDS = 600      # 10 minutes


# ==========================================================
# ROUTES
# ==========================================================

def home(request):
    return render(request, "chat/index.html")


def ask(request):
    q_raw = (request.POST.get("question", "") or "").strip()
    if not q_raw:
        return JsonResponse({"sql": "", "result": "", "error": "Please type something."})

    if not IBMI["user"] or not IBMI["password"]:
        return JsonResponse({"sql": "", "result": "", "error": "Missing IBM i credentials in env vars."})

    write_mode = q_raw.lower().startswith("write:")
    question = re.sub(r"(?is)^\s*write\s*:\s*", "", q_raw).strip()

    # If user wrote LIB/TABLE or LIB.TABLE, use that
    qual_lib, qual_tbl = _extract_qualified_table(question)
    library = (qual_lib or IBMI["library"]).upper()
    table = (qual_tbl or _extract_table_name(question))

    ssh = _ssh_connect()
    try:
        cols_map = _get_columns_map_cached(ssh, library)
        schema_text = _schema_for_prompt(library, table, cols_map)

        ai_sql = _unwrap_sql_fence(_ollama_make_sql(question, library, schema_text, write_mode))

        # If user specified LIB/TABLE in read mode, force FROM that table (prevents hallucination)
        if qual_lib and qual_tbl and not write_mode:
            ai_sql = _force_from_table(ai_sql, qual_lib, qual_tbl)
        else:
            ai_sql = _ensure_schema_qualified(ai_sql, library)

        ai_sql = _fix_sql_for_ctas(ai_sql)

        ok, final_sql_or_err = _validate_ai_sql(ai_sql, write_mode)
        if not ok:
            return JsonResponse({"sql": ai_sql, "result": "", "error": final_sql_or_err})

        final_sql = final_sql_or_err
        up = final_sql.upper().lstrip()

        if up.startswith("SELECT") or up.startswith("WITH"):
            out, err = _run_select_to_csv(ssh, final_sql, library)
            if not err.strip():
                out = _add_header_if_select_star(out, final_sql, cols_map)
        else:
            out, err = _run_sql_non_select(ssh, final_sql)

        return JsonResponse({"sql": final_sql, "result": out, "error": err})

    except requests.RequestException as e:
        return JsonResponse({"sql": "", "result": "", "error": f"Ollama error: {str(e)}"})
    except Exception as e:
        return JsonResponse({"sql": "", "result": "", "error": f"Server error: {str(e)}"})
    finally:
        try:
            ssh.close()
        except:
            pass


# ==========================================================
# SMALL HELPERS
# ==========================================================

def _extract_qualified_table(text: str):
    m = re.search(r"\b([A-Za-z0-9_]+)\s*[/.]\s*([A-Za-z0-9_]+)\b", text or "")
    return (m.group(1).upper(), m.group(2).upper()) if m else (None, None)


def _extract_table_name(text: str):
    for pat in [r"\bfrom\s+([A-Za-z0-9_]+)\b", r"\btable\s+([A-Za-z0-9_]+)\b"]:
        m = re.search(pat, text or "", flags=re.I)
        if m:
            return m.group(1).upper()
    return None


def _unwrap_sql_fence(text: str) -> str:
    t = (text or "").strip()
    m = SQL_BLOCK_RE.search(t)
    return m.group(1).strip() if m else t


# ==========================================================
# OLLAMA
# ==========================================================

def _ollama_make_sql(question: str, library: str, schema_text: str, write_mode: bool) -> str:
    mode_rules = (
        """
Write mode ENABLED.
Generate ONLY ONE statement: INSERT or UPDATE or DELETE (or SELECT if user asked).
- UPDATE/DELETE MUST include a WHERE clause.
- No semicolons. No multiple statements.
- No DDL (CREATE/ALTER/DROP/TRUNCATE), no CALL/GRANT/REVOKE.
""".strip()
        if write_mode else
        """
Write mode DISABLED.
Generate ONLY a SELECT (or WITH...SELECT). No INSERT/UPDATE/DELETE.
""".strip()
    )

    prompt = f"""
You are an expert IBM Db2 for i (AS/400) SQL assistant.
Return ONLY ONE SQL statement, nothing else.

Rules:
- Use schema-qualified tables like {library}.TABLE (or user-specified LIB/TABLE).
- Use only tables/columns from schema. Do not invent.
- For text filters like STATUS: use full values from user text (e.g., 'AVAILABLE', 'ISSUED') and do not guess codes.
- If using aggregates/expressions, always alias columns (COUNT(*) AS CNT).
- No semicolons. No multiple statements.
- Do NOT add FETCH FIRST / LIMIT clauses.

{mode_rules}

Schema:
{schema_text}

User request: {question}

SQL:
""".strip()

    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
        "options": {  # faster + more stable
            "temperature": 0.0,
            "top_p": 0.9,
            "num_predict": 80,
        },
    }

    r = requests.post(f"{OLLAMA_URL}/api/generate", json=payload, timeout=120)
    r.raise_for_status()
    return (r.json().get("response") or "").strip()


# ==========================================================
# SQL FIXUPS + VALIDATION
# ==========================================================

def _ensure_schema_qualified(sql: str, library: str) -> str:
    lib = library.upper()

    def repl_from(m):
        tbl = m.group(1)
        return f"FROM {tbl}" if "." in tbl else f"FROM {lib}.{tbl}"

    def repl_join(m):
        tbl = m.group(1)
        return f"JOIN {tbl}" if "." in tbl else f"JOIN {lib}.{tbl}"

    s = re.sub(r"\bFROM\s+([A-Za-z0-9_.]+)\b", repl_from, sql or "", flags=re.I)
    s = re.sub(r"\bJOIN\s+([A-Za-z0-9_.]+)\b", repl_join, s, flags=re.I)
    s = re.sub(rf"\b{lib}\.{lib}\.", f"{lib}.", s, flags=re.I)
    return s


def _force_from_table(sql: str, library: str, table: str) -> str:
    m = re.search(r"(?is)\bWHERE\b(.*)", sql or "")
    where_part = f" WHERE {m.group(1).strip()}" if m else ""
    return f"SELECT * FROM {library}.{table}{where_part}"


def _fix_sql_for_ctas(sql: str) -> str:
    s = (sql or "").strip()
    s = re.sub(r"(?is)\bSELECT\s+COUNT\s*\(\s*\*\s*\)(\s+FROM\b)", r"SELECT COUNT(*) AS CNT\1", s)
    s = re.sub(r"(?is)\bSELECT\s+COUNT\s*\(\s*([A-Za-z0-9_.]+)\s*\)(\s+FROM\b)", r"SELECT COUNT(\1) AS CNT\2", s)
    return s


def _validate_ai_sql(sql: str, write_mode: bool):
    s = (sql or "").strip()
    if not s:
        return False, "Empty SQL produced by AI."
    if ";" in s:
        return False, "Semicolons/multiple statements are not allowed."

    up = s.upper().lstrip()
    is_select = up.startswith("SELECT") or up.startswith("WITH")
    is_dml = up.startswith(("INSERT", "UPDATE", "DELETE", "MERGE"))

    banned = ["DROP", "ALTER", "CREATE", "TRUNCATE", "GRANT", "REVOKE", "CALL"]
    if any(b in up for b in banned):
        return False, "DDL/privileged commands are not allowed."

    if is_dml and not write_mode:
        return False, "Write queries are disabled. Use prefix `write:` to enable INSERT/UPDATE/DELETE."
    if not is_select and not is_dml:
        return False, "Only SELECT or INSERT/UPDATE/DELETE are allowed."
    if (up.startswith("UPDATE") or up.startswith("DELETE")) and " WHERE " not in up:
        return False, "UPDATE/DELETE must include a WHERE clause."

    return True, s


# ==========================================================
# SCHEMA (SMALL + CACHED)
# ==========================================================

def _get_columns_map_cached(ssh, library: str):
    lib = (library or "").upper()
    now = time.time()
    hit = _SCHEMA_CACHE.get(lib)
    if hit and (now - hit["ts"] < SCHEMA_TTL_SECONDS):
        return hit["cols_map"]

    cols_map = _get_columns_map(ssh, lib)
    _SCHEMA_CACHE[lib] = {"ts": now, "cols_map": cols_map}
    return cols_map


def _schema_for_prompt(library: str, table: str, cols_map: dict) -> str:
    lib = (library or "").upper()
    if not cols_map:
        return f"LIBRARY={lib}\n(schema unavailable)"

    # If we know the table, send only that table (fast + accurate)
    if table and table.upper() in cols_map:
        t = table.upper()
        cols = cols_map[t][:30]
        col_txt = ", ".join([f"{c} {dt}" for c, dt in cols])
        return f"LIBRARY={lib}\n{t}({col_txt})"

    # Otherwise small overview (not huge)
    tables = sorted(cols_map.keys())[:8]
    lines = [f"LIBRARY={lib}"]
    for t in tables:
        cols = cols_map[t][:12]
        col_txt = ", ".join([f"{c} {dt}" for c, dt in cols])
        lines.append(f"{t}({col_txt})")
    return "\n".join(lines)


# ==========================================================
# SSH + EXECUTION
# ==========================================================

def _ssh_connect():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(
        IBMI["host"],
        port=IBMI["port"],
        username=IBMI["user"],
        password=IBMI["password"]
    )
    return ssh


def _ssh_exec(ssh, cmd: str):
    _, stdout, stderr = ssh.exec_command(cmd)
    out = stdout.read().decode(errors="ignore")
    err = stderr.read().decode(errors="ignore")
    return out, err


def _run_select_to_csv(ssh, select_sql: str, library: str):
    tag = uuid.uuid4().hex[:8]
    sql_stmf = f"/tmp/ai_{tag}.sql"
    csv_stmf = f"/tmp/ai_{tag}.csv"
    log_stmf = f"/tmp/ai_{tag}.log"

    tbl = f"AIR{tag[:7]}".upper()  # max 10 chars

    script = f"""
. /QOpenSys/etc/profile
rm -f {sql_stmf} {csv_stmf} {log_stmf}

system "DLTF FILE({library}/{tbl})" 1>/dev/null 2>/dev/null

cat <<'EOF' > {sql_stmf}
CREATE TABLE {library}/{tbl} AS (
{select_sql}
) WITH DATA
EOF

system "RUNSQLSTM SRCSTMF('{sql_stmf}') COMMIT(*NONE) NAMING(*SYS)" 1>/dev/null 2>{log_stmf}
if [ $? -ne 0 ]; then
  echo "ERROR: RUNSQLSTM failed"
  cat {log_stmf}
  exit 1
fi

system "CPYTOIMPF FROMFILE({library}/{tbl}) TOSTMF('{csv_stmf}') MBROPT(*REPLACE) STMFCODPAG(*PCASCII) RCDDLM(*CRLF) DTAFMT(*DLM) FLDDLM(',')" 1>/dev/null 2>{log_stmf}
if [ $? -ne 0 ]; then
  echo "ERROR: CPYTOIMPF failed"
  cat {log_stmf}
  exit 1
fi

cat {csv_stmf}

system "DLTF FILE({library}/{tbl})" 1>/dev/null 2>/dev/null
"""
    return _ssh_exec(ssh, script)


def _run_sql_non_select(ssh, sql: str):
    tag = uuid.uuid4().hex[:8]
    sql_stmf = f"/tmp/ai_dml_{tag}.sql"
    script = f"""
. /QOpenSys/etc/profile
rm -f {sql_stmf}
cat <<EOF > {sql_stmf}
{sql}
EOF
system "RUNSQLSTM SRCSTMF('{sql_stmf}') COMMIT(*NONE) NAMING(*SYS)" 1>/dev/null 2>&1 || (echo "ERROR: RUNSQLSTM failed"; exit 1)
echo "OK"
"""
    return _ssh_exec(ssh, script)


def _get_columns_map(ssh, library: str):
    # NOTE: uses your same export path; cached so not called every request
    sql = (
        "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE "
        "FROM QSYS2.SYSCOLUMNS "
        f"WHERE TABLE_SCHEMA = '{library}' "
        "ORDER BY TABLE_NAME, ORDINAL_POSITION"
    )
    csv_text, err = _run_select_to_csv(ssh, sql, library)
    if err.strip():
        return {}

    rows = _parse_csv(csv_text)
    m = {}
    for r in rows:
        if len(r) < 3:
            continue
        t = r[0].strip().upper()
        c = r[1].strip().upper()
        dt = r[2].strip().upper()
        m.setdefault(t, []).append((c, dt))
    return m


def _parse_csv(text: str):
    text = (text or "").strip()
    if not text:
        return []
    return list(csv.reader(io.StringIO(text), delimiter=",", quotechar='"'))


def _add_header_if_select_star(csv_text: str, sql: str, cols_map: dict):
    m = re.search(r"(?is)^\s*SELECT\s+\*\s+FROM\s+([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)\b", sql or "")
    if not m or not csv_text:
        return csv_text

    tbl = m.group(2).upper()
    if tbl not in cols_map:
        return csv_text

    header = ",".join([c for (c, _dt) in cols_map[tbl]])
    return header + "\n" + csv_text
