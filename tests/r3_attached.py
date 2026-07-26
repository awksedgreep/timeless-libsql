#!/usr/bin/env python3
"""Attached-schema isolation regressions for every telemetry virtual table."""

import os
import sqlite3
import sys


def quote(identifier):
    return '"' + identifier.replace('"', '""') + '"'


def qualified(schema, table):
    return f"{quote(schema)}.{quote(table)}"


def connect(extension, main_path, attachments=()):
    db = sqlite3.connect(main_path, isolation_level=None)
    db.enable_load_extension(True)
    db.load_extension(extension)
    for schema, path in attachments:
        db.execute(f"ATTACH DATABASE ? AS {quote(schema)}", (path,))
    return db


def scalar(db, sql, params=()):
    return db.execute(sql, params).fetchone()[0]


def create_signal_tables(db, schema):
    db.execute(
        f"CREATE VIRTUAL TABLE {qualified(schema, 'metrics')} "
        "USING timeless_metrics"
    )
    db.execute(
        f"CREATE VIRTUAL TABLE {qualified(schema, 'logs')} "
        "USING timeless_logs(index_keys=service)"
    )
    db.execute(
        f"CREATE VIRTUAL TABLE {qualified(schema, 'traces')} "
        "USING timeless_traces"
    )


def flush(db, schema, table):
    db.execute(
        f"INSERT INTO {qualified(schema, table)}({quote(table)}) VALUES('flush')"
    )


def insert_signal_rows(db, schema, marker, base_ts):
    db.execute(
        f"INSERT INTO {qualified(schema, 'metrics')}(name, ts, value, labels) "
        "VALUES (?, ?, ?, ?)",
        (f"metric_{marker}", base_ts, 1.5, f'{{"schema":"{marker}"}}'),
    )
    db.execute(
        f"INSERT INTO {qualified(schema, 'logs')}"
        "(ts, level, message, metadata) VALUES (?, 'info', ?, ?)",
        (base_ts, f"log_{marker}", f'{{"service":"{marker}"}}'),
    )
    trace_byte = b"\x11" if marker == "main" else b"\x22"
    db.execute(
        f"INSERT INTO {qualified(schema, 'traces')}"
        "(trace_id, span_id, name, service, start_ts) VALUES (?, ?, ?, ?, ?)",
        (trace_byte * 16, trace_byte * 8, f"trace_{marker}", marker, base_ts),
    )
    for table in ("metrics", "logs", "traces"):
        flush(db, schema, table)


def assert_signal_rows(db, schema, marker):
    assert scalar(
        db,
        f"SELECT COUNT(*) FROM {qualified(schema, 'metrics')} WHERE name = ?",
        (f"metric_{marker}",),
    ) == 1
    assert scalar(
        db,
        f"SELECT COUNT(*) FROM {qualified(schema, 'logs')} WHERE message = ?",
        (f"log_{marker}",),
    ) == 1
    assert scalar(
        db,
        f"SELECT COUNT(*) FROM {qualified(schema, 'traces')} WHERE name = ?",
        (f"trace_{marker}",),
    ) == 1


def schema_objects(db, schema):
    return {
        (row[0], row[1])
        for row in db.execute(
            f"SELECT type, name FROM {quote(schema)}.sqlite_schema "
            "WHERE name NOT LIKE 'sqlite_%'"
        )
    }


def test_complete_isolation_and_lifecycle(extension, tmp):
    main_path = os.path.join(tmp, "main.db")
    aux_path = os.path.join(tmp, "aux.db")
    backup_path = os.path.join(tmp, "aux-backup.db")
    db = connect(extension, main_path, [("aux", aux_path)])

    create_signal_tables(db, "aux")
    expected_aux = {
        ("table", "metrics"),
        ("table", "metrics_chunks"),
        ("table", "metrics_meta"),
        ("table", "metrics_series"),
        ("index", "metrics_chunks_series_ts"),
        ("table", "logs"),
        ("table", "logs_blocks"),
        ("table", "logs_terms"),
        ("table", "logs_meta"),
        ("index", "logs_blocks_ts"),
        ("table", "traces"),
        ("table", "traces_blocks"),
        ("table", "traces_terms"),
        ("table", "traces_trace_blocks"),
        ("table", "traces_meta"),
        ("index", "traces_blocks_ts"),
    }
    assert schema_objects(db, "aux") == expected_aux
    assert schema_objects(db, "main") == set()

    # Identical names in main must create independent engines and shadow rows.
    create_signal_tables(db, "main")
    insert_signal_rows(db, "main", "main", 100)
    insert_signal_rows(db, "aux", "aux", 200)
    assert_signal_rows(db, "main", "main")
    assert_signal_rows(db, "aux", "aux")
    assert scalar(db, "SELECT COUNT(*) FROM main.metrics_chunks") == 1
    assert scalar(db, "SELECT COUNT(*) FROM aux.metrics_chunks") == 1
    assert scalar(db, "SELECT COUNT(*) FROM main.logs_blocks") == 1
    assert scalar(db, "SELECT COUNT(*) FROM aux.logs_blocks") == 1
    assert scalar(db, "SELECT COUNT(*) FROM main.traces_blocks") == 1
    assert scalar(db, "SELECT COUNT(*) FROM aux.traces_blocks") == 1

    # Maintenance on aux cannot touch the identically named main tables.
    for table, command in (
        ("metrics", "compact"),
        ("logs", "optimize"),
        ("traces", "optimize"),
    ):
        db.execute(
            f"INSERT INTO {qualified('aux', table)}({quote(table)}) VALUES(?)",
            (command,),
        )
    for table in ("metrics", "logs", "traces"):
        db.execute(
            f"INSERT INTO {qualified('aux', table)}({quote(table)}) VALUES('prune:150')"
        )
    assert_signal_rows(db, "main", "main")
    assert_signal_rows(db, "aux", "aux")

    db.execute("DETACH DATABASE aux")
    db.execute("ATTACH DATABASE ? AS aux", (aux_path,))
    assert_signal_rows(db, "main", "main")
    assert_signal_rows(db, "aux", "aux")

    # The attached file must be independently backup-able and reopen-able as
    # main, including its virtual-table definitions and all shadow state.
    backup = sqlite3.connect(backup_path, isolation_level=None)
    db.backup(backup, name="aux")
    backup.close()
    db.close()

    db = connect(extension, main_path, [("aux", aux_path)])
    assert_signal_rows(db, "main", "main")
    assert_signal_rows(db, "aux", "aux")

    backup = connect(extension, backup_path)
    assert_signal_rows(backup, "main", "aux")
    backup.close()

    # xDestroy must qualify every DROP, leaving same-named main state intact.
    for table in ("metrics", "logs", "traces"):
        db.execute(f"DROP TABLE {qualified('aux', table)}")
    assert schema_objects(db, "aux") == set()
    assert_signal_rows(db, "main", "main")
    db.close()


def test_quoted_schema_and_table_names(extension, tmp):
    main_path = os.path.join(tmp, "quoted-main.db")
    aux_path = os.path.join(tmp, 'quoted-aux.db')
    schema = 'aux"quoted'
    tables = {
        "metrics": 'metrics"quoted',
        "logs": 'logs"quoted',
        "traces": 'traces"quoted',
    }
    db = connect(extension, main_path, [(schema, aux_path)])

    db.execute(
        f"CREATE VIRTUAL TABLE {qualified(schema, tables['metrics'])} "
        "USING timeless_metrics"
    )
    db.execute(
        f"CREATE VIRTUAL TABLE {qualified(schema, tables['logs'])} "
        "USING timeless_logs"
    )
    db.execute(
        f"CREATE VIRTUAL TABLE {qualified(schema, tables['traces'])} "
        "USING timeless_traces"
    )
    db.execute(
        f"INSERT INTO {qualified(schema, tables['metrics'])}"
        "(name, ts, value) VALUES('quoted_metric', 1, 1)"
    )
    db.execute(
        f"INSERT INTO {qualified(schema, tables['logs'])}"
        "(ts, level, message) VALUES(1, 'info', 'quoted_log')"
    )
    db.execute(
        f"INSERT INTO {qualified(schema, tables['traces'])}"
        "(trace_id, span_id, name, service, start_ts) "
        "VALUES(zeroblob(16), zeroblob(8), 'quoted_trace', 'quoted', 1)"
    )
    for table in tables.values():
        flush(db, schema, table)

    assert scalar(
        db,
        f"SELECT COUNT(*) FROM {qualified(schema, tables['metrics'])} "
        "WHERE name='quoted_metric'",
    ) == 1
    assert scalar(
        db,
        f"SELECT COUNT(*) FROM {qualified(schema, tables['logs'])} "
        "WHERE message='quoted_log'",
    ) == 1
    assert scalar(
        db,
        f"SELECT COUNT(*) FROM {qualified(schema, tables['traces'])} "
        "WHERE name='quoted_trace'",
    ) == 1
    assert schema_objects(db, "main") == set()

    db.close()
    db = connect(extension, main_path, [(schema, aux_path)])
    assert scalar(
        db,
        f"SELECT COUNT(*) FROM {qualified(schema, tables['metrics'])}",
    ) == 1
    for table in tables.values():
        db.execute(f"DROP TABLE {qualified(schema, table)}")
    assert schema_objects(db, schema) == set()
    db.close()


def test_private_attached_schemas_do_not_share_engine(extension, _tmp):
    db = connect(extension, ":memory:", [("aux_mem", ":memory:")])
    db.execute("CREATE VIRTUAL TABLE main.metrics USING timeless_metrics")
    db.execute("CREATE VIRTUAL TABLE aux_mem.metrics USING timeless_metrics")
    db.execute(
        "INSERT INTO main.metrics(name, ts, value) VALUES('main_private', 1, 1)"
    )
    db.execute(
        "INSERT INTO aux_mem.metrics(name, ts, value) VALUES('aux_private', 2, 2)"
    )
    assert scalar(
        db, "SELECT COUNT(*) FROM main.metrics WHERE name='main_private'"
    ) == 1
    assert scalar(
        db, "SELECT COUNT(*) FROM main.metrics WHERE name='aux_private'"
    ) == 0
    assert scalar(
        db, "SELECT COUNT(*) FROM aux_mem.metrics WHERE name='aux_private'"
    ) == 1
    assert scalar(
        db, "SELECT COUNT(*) FROM aux_mem.metrics WHERE name='main_private'"
    ) == 0
    flush(db, "main", "metrics")
    flush(db, "aux_mem", "metrics")
    assert scalar(db, "SELECT COUNT(*) FROM main.metrics_chunks") == 1
    assert scalar(db, "SELECT COUNT(*) FROM aux_mem.metrics_chunks") == 1
    db.close()


def test_same_file_under_different_aliases_uses_local_schema(extension, tmp):
    telemetry_path = os.path.join(tmp, "aliased-telemetry.db")
    other_main_path = os.path.join(tmp, "aliased-other-main.db")
    owner = connect(extension, telemetry_path)
    create_signal_tables(owner, "main")
    insert_signal_rows(owner, "main", "main", 300)

    attached = connect(
        extension,
        other_main_path,
        [("renamed_telemetry", telemetry_path)],
    )
    assert_signal_rows(attached, "renamed_telemetry", "main")
    insert_signal_rows(attached, "renamed_telemetry", "aux", 400)
    assert_signal_rows(owner, "main", "aux")
    owner.close()
    attached.close()


def main():
    extension, tmp = sys.argv[1:]
    test_complete_isolation_and_lifecycle(extension, tmp)
    print("PASS test_complete_isolation_and_lifecycle")
    test_quoted_schema_and_table_names(extension, tmp)
    print("PASS test_quoted_schema_and_table_names")
    test_private_attached_schemas_do_not_share_engine(extension, tmp)
    print("PASS test_private_attached_schemas_do_not_share_engine")
    test_same_file_under_different_aliases_uses_local_schema(extension, tmp)
    print("PASS test_same_file_under_different_aliases_uses_local_schema")


if __name__ == "__main__":
    main()
