use pgrx::prelude::*;
use serde::Serialize;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

pgrx::pg_module_magic!();

#[derive(Serialize)]
struct DatabaseStats {
    timestamp: u64,
    total_connections: i64,
    active_connections: i64,
    idle_connections: i64,
    total_transactions: i64,
    commits: i64,
    rollbacks: i64,
    blocks_read: i64,
    blocks_hit: i64,
    tuples_returned: i64,
    tuples_fetched: i64,
    tuples_inserted: i64,
    tuples_updated: i64,
    tuples_deleted: i64,
    temp_files: i64,
    temp_bytes: i64,
    deadlocks: i64,
    block_read_time: f64,
    block_write_time: f64,
}

#[pg_extern]
fn collect_database_stats() -> Json<DatabaseStats> {
    Spi::connect(|client| {
        let stats = DatabaseStats {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            total_connections: query_single_value(
                client,
                "SELECT count(*) FROM pg_stat_activity",
            ),
            active_connections: query_single_value(
                client,
                "SELECT count(*) FROM pg_stat_activity WHERE state = 'active'",
            ),
            idle_connections: query_single_value(
                client,
                "SELECT count(*) FROM pg_stat_activity WHERE state = 'idle'",
            ),
            total_transactions: query_single_value(
                client,
                "SELECT xact_commit + xact_rollback FROM pg_stat_database WHERE datname = current_database()",
            ),
            commits: query_single_value(
                client,
                "SELECT xact_commit FROM pg_stat_database WHERE datname = current_database()",
            ),
            rollbacks: query_single_value(
                client,
                "SELECT xact_rollback FROM pg_stat_database WHERE datname = current_database()",
            ),
            blocks_read: query_single_value(
                client,
                "SELECT blks_read FROM pg_stat_database WHERE datname = current_database()",
            ),
            blocks_hit: query_single_value(
                client,
                "SELECT blks_hit FROM pg_stat_database WHERE datname = current_database()",
            ),
            tuples_returned: query_single_value(
                client,
                "SELECT tup_returned FROM pg_stat_database WHERE datname = current_database()",
            ),
            tuples_fetched: query_single_value(
                client,
                "SELECT tup_fetched FROM pg_stat_database WHERE datname = current_database()",
            ),
            tuples_inserted: query_single_value(
                client,
                "SELECT tup_inserted FROM pg_stat_database WHERE datname = current_database()",
            ),
            tuples_updated: query_single_value(
                client,
                "SELECT tup_updated FROM pg_stat_database WHERE datname = current_database()",
            ),
            tuples_deleted: query_single_value(
                client,
                "SELECT tup_deleted FROM pg_stat_database WHERE datname = current_database()",
            ),
            temp_files: query_single_value(
                client,
                "SELECT temp_files FROM pg_stat_database WHERE datname = current_database()",
            ),
            temp_bytes: query_single_value(
                client,
                "SELECT temp_bytes FROM pg_stat_database WHERE datname = current_database()",
            ),
            deadlocks: query_single_value(
                client,
                "SELECT deadlocks FROM pg_stat_database WHERE datname = current_database()",
            ),
            block_read_time: query_single_value_float(
                client,
                "SELECT blk_read_time FROM pg_stat_database WHERE datname = current_database()",
            ),
            block_write_time: query_single_value_float(
                client,
                "SELECT blk_write_time FROM pg_stat_database WHERE datname = current_database()",
            ),
        };
        
        Ok(Json(stats))
    })
    .unwrap()
}

fn query_single_value(client: &mut Spi, query: &str) -> i64 {
    client
        .select(query, None, None)
        .first()
        .get_one::<i64>()
        .unwrap()
        .unwrap_or(0)
}

fn query_single_value_float(client: &mut Spi, query: &str) -> f64 {
    client
        .select(query, None, None)
        .first()
        .get_one::<f64>()
        .unwrap()
        .unwrap_or(0.0)
}

#[pg_extern]
fn collect_table_stats() -> Json<HashMap<String, TableStats>> {
    Spi::connect(|client| {
        let mut table_stats = HashMap::new();
        
        let results = client.select(
            "SELECT schemaname, relname, 
                    seq_scan, seq_tup_read, 
                    idx_scan, idx_tup_fetch,
                    n_tup_ins, n_tup_upd, n_tup_del,
                    n_live_tup, n_dead_tup,
                    heap_blks_read, heap_blks_hit,
                    idx_blks_read, idx_blks_hit
             FROM pg_stat_user_tables",
            None,
            None,
        );

        for row in results {
            let schema: String = row.get_by_name("schemaname").unwrap().unwrap();
            let table: String = row.get_by_name("relname").unwrap().unwrap();
            let key = format!("{}.{}", schema, table);
            
            table_stats.insert(key, TableStats {
                sequential_scans: row.get_by_name("seq_scan").unwrap().unwrap(),
                sequential_rows_read: row.get_by_name("seq_tup_read").unwrap().unwrap(),
                index_scans: row.get_by_name("idx_scan").unwrap().unwrap(),
                index_rows_fetched: row.get_by_name("idx_tup_fetch").unwrap().unwrap(),
                rows_inserted: row.get_by_name("n_tup_ins").unwrap().unwrap(),
                rows_updated: row.get_by_name("n_tup_upd").unwrap().unwrap(),
                rows_deleted: row.get_by_name("n_tup_del").unwrap().unwrap(),
                live_rows: row.get_by_name("n_live_tup").unwrap().unwrap(),
                dead_rows: row.get_by_name("n_dead_tup").unwrap().unwrap(),
                heap_blocks_read: row.get_by_name("heap_blks_read").unwrap().unwrap(),
                heap_blocks_hit: row.get_by_name("heap_blks_hit").unwrap().unwrap(),
                index_blocks_read: row.get_by_name("idx_blks_read").unwrap().unwrap(),
                index_blocks_hit: row.get_by_name("idx_blks_hit").unwrap().unwrap(),
            });
        }
        
        Ok(Json(table_stats))
    })
    .unwrap()
}

#[derive(Serialize)]
struct TableStats {
    sequential_scans: i64,
    sequential_rows_read: i64,
    index_scans: i64,
    index_rows_fetched: i64,
    rows_inserted: i64,
    rows_updated: i64,
    rows_deleted: i64,
    live_rows: i64,
    dead_rows: i64,
    heap_blocks_read: i64,
    heap_blocks_hit: i64,
    index_blocks_read: i64,
    index_blocks_hit: i64,
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

     #[pg_test]
    fn test_hello_pg_sweep() {
        assert_eq!("Hello, pg_sweep", crate::hello_pg_sweep());
    }

    #[pg_test]
    fn test_collect_database_stats() {
        let stats = crate::collect_database_stats();
        assert!(stats.0.total_connections >= 0);
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // Initialize test environment
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![
            "track_activities=on",
            "track_counts=on",
            "track_io_timing=on",
        ]
    }
}