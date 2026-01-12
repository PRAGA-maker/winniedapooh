import sqlite3
import json
import time
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from contextlib import contextmanager


class ResultsDatabase:
    """
    SQLite database for indexing and querying experiment runs.
    
    Provides a queryable interface to experiment results stored in data/outputs/.
    The database makes it easy to browse runs, compare methods, and analyze performance
    without manually navigating the file system.
    
    Usage:
        db = ResultsDatabase()
        db.index_run(run_dir, spec, metrics)
        runs = db.list_runs(method="last_price", limit=10)
    """
    
    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize the results database.
        
        Args:
            db_path: Path to SQLite database file (defaults to data/results.db)
        """
        if db_path is None:
            db_path = "data/results.db"
        
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Initialize schema if needed
        self._init_schema()
    
    @contextmanager
    def _connect(self):
        """Context manager for database connections."""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()
    
    def _init_schema(self):
        """Create database schema if it doesn't exist."""
        with self._connect() as conn:
            cursor = conn.cursor()
            
            # Create runs table with flattened metrics
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    run_name TEXT NOT NULL,
                    method TEXT NOT NULL,
                    task TEXT NOT NULL,
                    dataset_path TEXT NOT NULL,
                    timestamp INTEGER NOT NULL,
                    seed INTEGER NOT NULL,
                    method_params TEXT,
                    task_params TEXT,
                    test_brier REAL,
                    test_logloss REAL,
                    bench_brier REAL,
                    bench_logloss REAL,
                    run_dir TEXT NOT NULL,
                    spec_path TEXT NOT NULL,
                    metrics_path TEXT NOT NULL,
                    model_path TEXT NOT NULL,
                    description TEXT
                )
            """)
            
            # Create indices for fast queries
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_method ON runs(method)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_task ON runs(task)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON runs(timestamp)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_test_brier ON runs(test_brier)")
            
            conn.commit()
    
    def index_run(self, run_dir: Path, spec: Dict[str, Any], metrics: Dict[str, Dict[str, float]]):
        """
        Index a completed experiment run.
        
        Args:
            run_dir: Path to the run directory
            spec: RunSpec dictionary containing run configuration
            metrics: Dictionary with 'test' and 'bench' metric dictionaries
        """
        try:
            # Generate run_id from directory structure
            # Format: {method}_{config_hash}/run_{timestamp}_{name}
            run_id = f"{run_dir.parent.name}/{run_dir.name}"
            
            # Extract timestamp from run directory name
            # run_{timestamp}_{name}
            parts = run_dir.name.split("_", 2)
            if len(parts) >= 2:
                timestamp = int(parts[1])
            else:
                timestamp = int(time.time())
            
            # Extract metrics (handle missing metrics gracefully)
            test_metrics = metrics.get("test", {})
            bench_metrics = metrics.get("bench", {})
            
            # Prepare paths
            spec_path = str(run_dir / "metrics" / "spec.json")
            metrics_path = str(run_dir / "metrics" / "metrics.json")
            model_path = str(run_dir / "model" / "model.joblib")
            
            with self._connect() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT OR REPLACE INTO runs (
                        run_id, run_name, method, task, dataset_path, timestamp, seed,
                        method_params, task_params,
                        test_brier, test_logloss, bench_brier, bench_logloss,
                        run_dir, spec_path, metrics_path, model_path, description
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    run_id,
                    spec.get("run_name", ""),
                    spec.get("method", ""),
                    spec.get("task", ""),
                    spec.get("dataset_path", ""),
                    timestamp,
                    spec.get("seed", 42),
                    json.dumps(spec.get("method_params", {})),
                    json.dumps(spec.get("task_params", {})),
                    test_metrics.get("brier"),
                    test_metrics.get("logloss"),
                    bench_metrics.get("brier"),
                    bench_metrics.get("logloss"),
                    str(run_dir),
                    spec_path,
                    metrics_path,
                    model_path,
                    spec.get("description", "")
                ))
                conn.commit()
        except Exception as e:
            # Fail gracefully - don't break the experiment run
            print(f"Warning: Failed to index run to database: {e}")
    
    def list_runs(
        self, 
        limit: int = 10,
        method: Optional[str] = None,
        task: Optional[str] = None,
        since_days: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        List experiment runs with optional filters.
        
        Args:
            limit: Maximum number of runs to return
            method: Filter by method name
            task: Filter by task name
            since_days: Only show runs from the last N days
            
        Returns:
            List of run dictionaries, sorted by timestamp (newest first)
        """
        query = "SELECT * FROM runs WHERE 1=1"
        params = []
        
        if method:
            query += " AND method = ?"
            params.append(method)
        
        if task:
            query += " AND task = ?"
            params.append(task)
        
        if since_days:
            cutoff = int(time.time()) - (since_days * 86400)
            query += " AND timestamp >= ?"
            params.append(cutoff)
        
        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)
        
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            return [dict(row) for row in rows]
    
    def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information about a specific run.
        
        Args:
            run_id: Run identifier
            
        Returns:
            Run dictionary or None if not found
        """
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,))
            row = cursor.fetchone()
            
            if row:
                return dict(row)
            return None
    
    def compare_runs(self, run_ids: List[str]) -> List[Dict[str, Any]]:
        """
        Get comparison data for multiple runs.
        
        Args:
            run_ids: List of run identifiers
            
        Returns:
            List of run dictionaries in the same order as run_ids
        """
        if not run_ids:
            return []
        
        placeholders = ",".join("?" * len(run_ids))
        query = f"SELECT * FROM runs WHERE run_id IN ({placeholders})"
        
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(query, run_ids)
            rows = cursor.fetchall()
            
            # Create a mapping for quick lookup
            runs_dict = {row["run_id"]: dict(row) for row in rows}
            
            # Return in the same order as input
            return [runs_dict[rid] for rid in run_ids if rid in runs_dict]
    
    def get_methods_comparison(
        self,
        task: Optional[str] = None,
        latest_only: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Get all runs for comparing methods.
        
        Args:
            task: Filter by task name
            latest_only: If True, only return the latest run for each method
            
        Returns:
            List of run dictionaries
        """
        if latest_only:
            # Get the latest run for each method
            query = """
                SELECT r1.* FROM runs r1
                INNER JOIN (
                    SELECT method, MAX(timestamp) as max_ts
                    FROM runs
                    WHERE 1=1
            """
            params = []
            
            if task:
                query += " AND task = ?"
                params.append(task)
            
            query += """
                    GROUP BY method
                ) r2 ON r1.method = r2.method AND r1.timestamp = r2.max_ts
                ORDER BY r1.method
            """
        else:
            query = "SELECT * FROM runs WHERE 1=1"
            params = []
            
            if task:
                query += " AND task = ?"
                params.append(task)
            
            query += " ORDER BY method, timestamp DESC"
        
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            return [dict(row) for row in rows]
    
    def get_method_runs(self, method: str) -> List[Dict[str, Any]]:
        """
        Get all runs for a specific method.
        
        Args:
            method: Method name
            
        Returns:
            List of run dictionaries, sorted by timestamp (newest first)
        """
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM runs WHERE method = ? ORDER BY timestamp DESC",
                (method,)
            )
            rows = cursor.fetchall()
            
            return [dict(row) for row in rows]
    
    def get_metrics_summary(
        self,
        method: Optional[str] = None,
        task: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get aggregated metrics summary.
        
        Args:
            method: Filter by method name
            task: Filter by task name
            
        Returns:
            Dictionary with summary statistics
        """
        query = """
            SELECT 
                method,
                COUNT(*) as run_count,
                AVG(test_brier) as avg_test_brier,
                MIN(test_brier) as min_test_brier,
                MAX(test_brier) as max_test_brier,
                AVG(bench_brier) as avg_bench_brier,
                MIN(bench_brier) as min_bench_brier,
                MAX(bench_brier) as max_bench_brier
            FROM runs
            WHERE 1=1
        """
        params = []
        
        if method:
            query += " AND method = ?"
            params.append(method)
        
        if task:
            query += " AND task = ?"
            params.append(task)
        
        query += " GROUP BY method ORDER BY method"
        
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            return [dict(row) for row in rows]
    
    def count_runs(self) -> int:
        """Get total number of runs in the database."""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM runs")
            return cursor.fetchone()[0]


# LESSONS LEARNED:
# 1. Flattening metrics into separate columns makes queries much simpler and faster
#    than storing JSON and using json_extract(). The slight denormalization is worth it.
# 2. Using REPLACE instead of INSERT allows re-indexing runs without errors if something
#    changes in the output directory structure.
# 3. Fail-safe indexing (catching exceptions in index_run) prevents database issues from
#    breaking experiment runs. Logging warnings is enough - the run data is still saved.
# 4. Context manager for connections ensures proper cleanup even if queries fail.
# 5. Indices on method, task, timestamp make common queries very fast (<1ms typically).
# 6. Using run_id as "{method_config}/run_timestamp_name" creates a natural hierarchy
#    that matches the filesystem structure and groups related runs together.
