use std::path::Path;

use anyhow::{bail, ensure, Result};
use rusqlite::{params, Connection};

use super::super::open;

fn scalar(connection: &Connection, sql: &str) -> Result<i64> {
    Ok(connection.query_row(sql, [], |row| row.get(0))?)
}

fn expect_error(connection: &Connection, sql: &str, label: &str) -> Result<()> {
    if connection.execute_batch(sql).is_ok() {
        bail!("{label}: statement unexpectedly succeeded");
    }
    Ok(())
}

struct StatementCase {
    table: &'static str,
    insert: &'static str,
    count: &'static str,
}

const STATEMENT_CASES: [StatementCase; 3] = [
    StatementCase {
        table: "metrics",
        insert: "INSERT INTO metrics(name,ts,value) VALUES ('stmt_bad',10,1.0),('stmt_bad',11,NULL)",
        count: "SELECT COUNT(*) FROM metrics WHERE name='stmt_bad'",
    },
    StatementCase {
        table: "logs",
        insert: "INSERT INTO logs(ts,level,message) VALUES (10,'info','stmt_bad'),(11,'fatal','stmt_bad')",
        count: "SELECT COUNT(*) FROM logs WHERE message='stmt_bad'",
    },
    StatementCase {
        table: "traces",
        insert: "INSERT INTO traces(trace_id,span_id,name,service,start_ts) VALUES (zeroblob(16),zeroblob(8),'stmt_bad','svc',10),(x'01',zeroblob(8),'stmt_bad','svc',11)",
        count: "SELECT COUNT(*) FROM traces WHERE name='stmt_bad'",
    },
];

struct SavepointCase {
    table: &'static str,
    outer: &'static str,
    inner: &'static str,
    outer_count: &'static str,
    inner_count: &'static str,
}

const SAVEPOINT_CASES: [SavepointCase; 3] = [
    SavepointCase {
        table: "metrics",
        outer: "INSERT INTO metrics(name,ts,value) VALUES ('save_outer',20,2.0)",
        inner: "INSERT INTO metrics(name,ts,value) VALUES ('save_inner',21,3.0)",
        outer_count: "SELECT COUNT(*) FROM metrics WHERE name='save_outer'",
        inner_count: "SELECT COUNT(*) FROM metrics WHERE name='save_inner'",
    },
    SavepointCase {
        table: "logs",
        outer: "INSERT INTO logs(ts,level,message) VALUES (20,'info','save_outer')",
        inner: "INSERT INTO logs(ts,level,message) VALUES (21,'error','save_inner')",
        outer_count: "SELECT COUNT(*) FROM logs WHERE message='save_outer'",
        inner_count: "SELECT COUNT(*) FROM logs WHERE message='save_inner'",
    },
    SavepointCase {
        table: "traces",
        outer: "INSERT INTO traces(trace_id,span_id,name,service,start_ts) VALUES (x'11111111111111111111111111111111',x'1111111111111111','save_outer','svc',20)",
        inner: "INSERT INTO traces(trace_id,span_id,name,service,start_ts) VALUES (x'22222222222222222222222222222222',x'2222222222222222','save_inner','svc',21)",
        outer_count: "SELECT COUNT(*) FROM traces WHERE name='save_outer'",
        inner_count: "SELECT COUNT(*) FROM traces WHERE name='save_inner'",
    },
];

struct BoundaryCase {
    table: &'static str,
    insert: &'static str,
    count: &'static str,
}

const BOUNDARY_CASES: [BoundaryCase; 3] = [
    BoundaryCase {
        table: "metrics",
        insert: "INSERT INTO metrics(name,ts,value) VALUES ('boundary_row',30,3.0)",
        count: "SELECT COUNT(*) FROM metrics WHERE name='boundary_row'",
    },
    BoundaryCase {
        table: "logs",
        insert: "INSERT INTO logs(ts,level,message) VALUES (30,'info','boundary_row')",
        count: "SELECT COUNT(*) FROM logs WHERE message='boundary_row'",
    },
    BoundaryCase {
        table: "traces",
        insert: "INSERT INTO traces(trace_id,span_id,name,service,start_ts) VALUES (x'30303030303030303030303030303030',x'3030303030303030','boundary_row','svc',30)",
        count: "SELECT COUNT(*) FROM traces WHERE name='boundary_row'",
    },
];

struct MaintenanceCase {
    table: &'static str,
    outer: &'static str,
    commands: [&'static str; 3],
    row_count: &'static str,
    block_count: &'static str,
}

const MAINTENANCE_CASES: [MaintenanceCase; 3] = [
    MaintenanceCase {
        table: "metrics_maint",
        outer: "INSERT INTO metrics_maint(name,ts,value) VALUES ('outer',3,3.0)",
        commands: [
            "INSERT INTO metrics_maint(metrics_maint) VALUES ('flush')",
            "INSERT INTO metrics_maint(metrics_maint) VALUES ('compact')",
            "INSERT INTO metrics_maint(metrics_maint) VALUES ('prune:1000')",
        ],
        row_count: "SELECT COUNT(*) FROM metrics_maint",
        block_count: "SELECT COUNT(*) FROM metrics_maint_chunks",
    },
    MaintenanceCase {
        table: "logs_maint",
        outer: "INSERT INTO logs_maint(ts,level,message) VALUES (3,'info','outer')",
        commands: [
            "INSERT INTO logs_maint(logs_maint) VALUES ('flush')",
            "INSERT INTO logs_maint(logs_maint) VALUES ('optimize')",
            "INSERT INTO logs_maint(logs_maint) VALUES ('prune:1000')",
        ],
        row_count: "SELECT COUNT(*) FROM logs_maint",
        block_count: "SELECT COUNT(*) FROM logs_maint_blocks",
    },
    MaintenanceCase {
        table: "traces_maint",
        outer: "INSERT INTO traces_maint(trace_id,span_id,name,service,start_ts) VALUES (x'03030303030303030303030303030303',x'0303030303030303','outer','svc',3)",
        commands: [
            "INSERT INTO traces_maint(traces_maint) VALUES ('flush')",
            "INSERT INTO traces_maint(traces_maint) VALUES ('optimize')",
            "INSERT INTO traces_maint(traces_maint) VALUES ('prune:1000')",
        ],
        row_count: "SELECT COUNT(*) FROM traces_maint",
        block_count: "SELECT COUNT(*) FROM traces_maint_blocks",
    },
];

pub(super) fn run(extension: &Path, temporary: &Path) -> Result<()> {
    let database = temporary.join("r1.db");
    let connection = open(extension, &database)?;
    connection.execute_batch(
        "CREATE VIRTUAL TABLE metrics USING timeless_metrics;
         CREATE VIRTUAL TABLE logs USING timeless_logs;
         CREATE VIRTUAL TABLE traces USING timeless_traces;
         CREATE VIRTUAL TABLE metrics_threshold USING timeless_metrics;
         WITH RECURSIVE seq(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM seq WHERE n<4095)
         INSERT INTO metrics_threshold(name,ts,value)
         SELECT 'threshold_metric',100000+n,n FROM seq;",
    )?;
    let metric_stat = |key: &str| -> Result<i64> {
        Ok(connection.query_row(
            "SELECT CAST(value AS INTEGER) FROM timeless_stats('metrics_threshold') WHERE key=?1",
            params![key],
            |row| row.get(0),
        )?)
    };
    ensure!(metric_stat("buffered_points")? == 4095);
    ensure!(metric_stat("disk_points")? == 0);
    ensure!(scalar(&connection, "SELECT COUNT(*) FROM metrics_threshold_chunks")? == 0);
    connection.execute(
        "INSERT INTO metrics_threshold(name,ts,value) VALUES ('threshold_metric',104096,4096)",
        [],
    )?;
    ensure!(metric_stat("buffered_points")? == 0);
    ensure!(metric_stat("disk_points")? == 4096);
    ensure!(scalar(&connection, "SELECT COUNT(*) FROM metrics_threshold_chunks")? == 1);

    for case in STATEMENT_CASES {
        connection.execute("BEGIN", [])?;
        expect_error(&connection, case.insert, case.table)?;
        ensure!(
            !connection.is_autocommit(),
            "{} ended outer transaction",
            case.table
        );
        ensure!(
            scalar(&connection, case.count)? == 0,
            "{} leaked failed row",
            case.table
        );
        connection.execute("COMMIT", [])?;
    }
    for case in STATEMENT_CASES {
        let insert = case.insert.replace("stmt_bad", "autocommit_bad");
        let count = case.count.replace("stmt_bad", "autocommit_bad");
        expect_error(&connection, &insert, case.table)?;
        ensure!(
            connection.is_autocommit(),
            "{} retained failed transaction",
            case.table
        );
        ensure!(
            scalar(&connection, &count)? == 0,
            "{} leaked autocommit row",
            case.table
        );
    }

    let threshold_cases = [
        (
            "logs",
            "WITH RECURSIVE seq(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM seq WHERE n<8200) INSERT INTO logs(ts,level,message) SELECT 10000+n,CASE WHEN n=8200 THEN 'fatal' ELSE 'info' END,'threshold_bad' FROM seq",
            "SELECT COUNT(*) FROM logs WHERE message='threshold_bad'",
            "SELECT COUNT(*) FROM logs_blocks",
        ),
        (
            "traces",
            "WITH RECURSIVE seq(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM seq WHERE n<8200) INSERT INTO traces(trace_id,span_id,name,service,start_ts) SELECT CASE WHEN n=8200 THEN x'01' ELSE zeroblob(16) END,zeroblob(8),'threshold_bad','svc',10000+n FROM seq",
            "SELECT COUNT(*) FROM traces WHERE name='threshold_bad'",
            "SELECT COUNT(*) FROM traces_blocks",
        ),
    ];
    for (table, insert, count, blocks) in threshold_cases {
        connection.execute("BEGIN", [])?;
        expect_error(&connection, insert, table)?;
        ensure!(scalar(&connection, count)? == 0, "{table} leaked rows");
        ensure!(scalar(&connection, blocks)? == 0, "{table} leaked blocks");
        connection.execute("COMMIT", [])?;
    }

    for case in SAVEPOINT_CASES {
        connection.execute("BEGIN", [])?;
        connection.execute(case.outer, [])?;
        connection.execute(&format!("SAVEPOINT {}_sp", case.table), [])?;
        connection.execute(case.inner, [])?;
        connection.execute(&format!("ROLLBACK TO {}_sp", case.table), [])?;
        connection.execute(&format!("RELEASE {}_sp", case.table), [])?;
        ensure!(!connection.is_autocommit());
        connection.execute("COMMIT", [])?;
        ensure!(scalar(&connection, case.outer_count)? == 1);
        ensure!(scalar(&connection, case.inner_count)? == 0);
    }
    for case in BOUNDARY_CASES {
        connection.execute("BEGIN", [])?;
        connection.execute(&format!("SAVEPOINT {}_before_begin", case.table), [])?;
        connection.execute(&case.insert.replace("boundary_row", "late_participant"), [])?;
        connection.execute(&format!("ROLLBACK TO {}_before_begin", case.table), [])?;
        connection.execute(&format!("RELEASE {}_before_begin", case.table), [])?;
        connection.execute("COMMIT", [])?;
        ensure!(
            scalar(
                &connection,
                &case.count.replace("boundary_row", "late_participant")
            )? == 0
        );
    }
    for case in BOUNDARY_CASES {
        connection.execute("BEGIN", [])?;
        connection.execute(&format!("SAVEPOINT {}_released", case.table), [])?;
        connection.execute(
            &case
                .insert
                .replace("boundary_row", "released_then_rolled_back"),
            [],
        )?;
        connection.execute(&format!("RELEASE {}_released", case.table), [])?;
        connection.execute("ROLLBACK", [])?;
        ensure!(
            scalar(
                &connection,
                &case
                    .count
                    .replace("boundary_row", "released_then_rolled_back")
            )? == 0
        );
    }

    connection.execute_batch(
        "CREATE VIRTUAL TABLE metrics_maint USING timeless_metrics;
         CREATE VIRTUAL TABLE logs_maint USING timeless_logs;
         CREATE VIRTUAL TABLE traces_maint USING timeless_traces;
         INSERT INTO metrics_maint(name,ts,value) VALUES ('old',1,1.0);
         INSERT INTO metrics_maint(metrics_maint) VALUES ('flush');
         INSERT INTO metrics_maint(name,ts,value) VALUES ('buffered',2,2.0);
         INSERT INTO logs_maint(ts,level,message) VALUES (1,'info','old');
         INSERT INTO logs_maint(logs_maint) VALUES ('flush');
         INSERT INTO logs_maint(ts,level,message) VALUES (2,'info','buffered');
         INSERT INTO traces_maint(trace_id,span_id,name,service,start_ts) VALUES (x'01010101010101010101010101010101',x'0101010101010101','old','svc',1);
         INSERT INTO traces_maint(traces_maint) VALUES ('flush');
         INSERT INTO traces_maint(trace_id,span_id,name,service,start_ts) VALUES (x'02020202020202020202020202020202',x'0202020202020202','buffered','svc',2);",
    )?;
    for case in MAINTENANCE_CASES {
        connection.execute("BEGIN", [])?;
        connection.execute(case.outer, [])?;
        connection.execute(&format!("SAVEPOINT {}_maintenance", case.table), [])?;
        for command in case.commands {
            connection.execute(command, [])?;
        }
        ensure!(scalar(&connection, case.row_count)? == 0);
        connection.execute(&format!("ROLLBACK TO {}_maintenance", case.table), [])?;
        connection.execute(&format!("RELEASE {}_maintenance", case.table), [])?;
        connection.execute("COMMIT", [])?;
        ensure!(scalar(&connection, case.row_count)? == 3);
        ensure!(scalar(&connection, case.block_count)? == 1);
    }

    connection.execute_batch(
        "INSERT INTO metrics(metrics) VALUES ('flush');
         INSERT INTO logs(logs) VALUES ('flush');
         INSERT INTO traces(traces) VALUES ('flush');
         INSERT INTO metrics_maint(metrics_maint) VALUES ('flush');
         INSERT INTO logs_maint(logs_maint) VALUES ('flush');
         INSERT INTO traces_maint(traces_maint) VALUES ('flush');",
    )?;
    drop(connection);

    let connection = open(extension, &database)?;
    for case in STATEMENT_CASES {
        ensure!(scalar(&connection, case.count)? == 0);
        ensure!(
            scalar(
                &connection,
                &case.count.replace("stmt_bad", "autocommit_bad")
            )? == 0
        );
    }
    for (_, _, count, _) in threshold_cases {
        ensure!(scalar(&connection, count)? == 0);
    }
    for case in SAVEPOINT_CASES {
        ensure!(scalar(&connection, case.outer_count)? == 1);
        ensure!(scalar(&connection, case.inner_count)? == 0);
    }
    for case in BOUNDARY_CASES {
        for tag in ["late_participant", "released_then_rolled_back"] {
            ensure!(scalar(&connection, &case.count.replace("boundary_row", tag))? == 0);
        }
    }
    for case in MAINTENANCE_CASES {
        ensure!(scalar(&connection, case.row_count)? == 3);
    }
    println!("PASS: statement, savepoint, auto-flush, and maintenance rollback are atomic");
    Ok(())
}
