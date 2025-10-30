#!/usr/bin/env python3

"""
Query Planner Challenge
-----------------------------------------------

  1. Columnar storage (Parquet) with Hive partitioning for predicate pushdown
  2. Pre-computed materialized views (partial OLAP cube)
  3. Query rewriting with materialized view selection

Prepare phase:
  - Streams CSV → Parquet
  - Partitions by event type for file-level predicate pushdown
  - Pre-computes aggregations at multiple granularities

Run phase:
  - Analyzes incoming queries and rewrites them to use optimal data sources
  - Routes queries to pre-computed aggregates when possible (KB vs GB reads)
  - Falls back to full partitioned Parquet scan

Usage:
  # Prepare phase
  python query-planner-challenge.py --data-dir ./data --prepared-dir ./prepared --prepare
  
  # Run phase
  python query-planner-challenge.py --prepared-dir ./prepared --out-dir ./results --run
  
  # Run both phases sequentially
  python query-planner-challenge.py --data-dir ./data --prepared-dir ./prepared --out-dir ./results --prepare --run
"""

import duckdb
import time
from pathlib import Path
import argparse
from inputs import queries
from assembler import assemble_sql
# from judges import queries

# -------------------
# Load Data
# -------------------
def prepare_phase(data_dir: Path, out_dir: Path):
    """
    Prepare phase – You’re given the dataset. You may load, index, transform, or pre-aggregate it in any way to create your optimized data store or indexes. 

    """
    out_dir.mkdir(parents=True, exist_ok=True)
    tmp_dir = out_dir / "tmp"
    tmp_dir.mkdir(exist_ok=True)
    
    con = duckdb.connect()
    
    con.execute("SET threads TO 8;") # Macbook Air M2 has 8-core CPU with 4 performance cores and 4 efficiency cores
    con.execute("SET memory_limit = '12GB';") # Defaults to 80% of RAM, so 12.8 GB. Set memory limit manually later
    con.execute(f"SET temp_directory = '{tmp_dir}';") # Configures disk spilling if queries exceed memory
    
    csv_path = str(data_dir / "events_part_*.csv")
    partitioned_path = out_dir / "events_partitioned"
    
    print(f"🟩 Converting CSV from {csv_path} to Parquet at {partitioned_path} ...")
    t0 = time.time()
    
    # Instead of a saved view, do a one time COPY of csv to a Parquet file.
    # Columnar physical data model is best for OLAP such as this (core characteristic of OLAP vs OLTP is that OLAP accesses many rows, but few columns and performs aggregations or filters over them)
    # Use ZSTD compression to reduce main I/O bandwidth bottleneck (check if there are better compression algorithms for this use case)
    # Use Hive partitioning for predicate pushdown later (skips entire row groups by type)
    con.execute(f"""
        COPY (
            SELECT 
                to_timestamp(TRY_CAST(ts AS BIGINT) / 1000) AS ts,
                type,
                auction_id,
                TRY_CAST(advertiser_id AS INTEGER) AS advertiser_id,
                TRY_CAST(publisher_id AS INTEGER) AS publisher_id,
                TRY_CAST(NULLIF(bid_price, '') AS DOUBLE) AS bid_price,
                TRY_CAST(user_id AS BIGINT) AS user_id,
                TRY_CAST(NULLIF(total_price, '') AS DOUBLE) AS total_price,
                country,
                DATE(to_timestamp(TRY_CAST(ts AS BIGINT) / 1000)) AS day,
                DATE_TRUNC('week', to_timestamp(TRY_CAST(ts AS BIGINT) / 1000)) AS week,
                DATE_TRUNC('hour', to_timestamp(TRY_CAST(ts AS BIGINT) / 1000)) AS hour,
                STRFTIME(to_timestamp(TRY_CAST(ts AS BIGINT) / 1000), '%Y-%m-%d %H:%M') AS minute
            FROM read_csv('{csv_path}', 
                          AUTO_DETECT=FALSE,
                          HEADER=TRUE, 
                          union_by_name=TRUE,
                          parallel=TRUE,
                          COLUMNS={{
                            'ts': 'VARCHAR',
                            'type': 'VARCHAR',
                            'auction_id': 'VARCHAR',
                            'advertiser_id': 'VARCHAR',
                            'publisher_id': 'VARCHAR',
                            'bid_price': 'VARCHAR',
                            'user_id': 'VARCHAR',
                            'total_price': 'VARCHAR',
                            'country': 'VARCHAR'
                          }})
        )
        TO '{partitioned_path}' (FORMAT 'PARQUET', 
                                 PARTITION_BY (type),
                                 COMPRESSION 'ZSTD',
                                 ROW_GROUP_SIZE 100000)
    """)
    print(f"✅ Converted CSV to Parquet in {time.time() - t0:.2f}s")
    
    # Pre-compute aggregations using materialized views
    # Only partial OLAP cube. Pre-computing all possible dimensions isn't possible due to 20GB dataset size and 100 GB disk usage constraint.
    # If there's time, optimize which aggregations to pre-compute based on the queries + unseen queries.
    print("🟩 Pre-computing aggregations...")
    t0 = time.time()
    
    con.execute(f"""
        CREATE VIEW events AS 
        SELECT * FROM read_parquet('{partitioned_path}/**/*.parquet', 
                                    hive_partitioning=1);
    """)
    
    adv_counts_path = out_dir / "adv_type_counts.parquet"
    con.execute(f"""
        COPY (SELECT advertiser_id, type, COUNT(*) AS count_star
              FROM events
              GROUP BY advertiser_id, type)
        TO '{adv_counts_path}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD');
    """)
    
    purchase_by_country_path = out_dir / "purchase_country_stats.parquet"
    con.execute(f"""
        COPY (SELECT country, 
                     SUM(total_price) AS total_revenue, 
                     COUNT(*) AS purchase_count
              FROM events
              WHERE type = 'purchase'
              GROUP BY country)
        TO '{purchase_by_country_path}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD');
    """)
    
    impressions_by_day_path = out_dir / "impression_day_revenue.parquet"
    con.execute(f"""
        COPY (SELECT day, SUM(bid_price) AS sum_bid_price
              FROM events
              WHERE type = 'impression'
              GROUP BY day
              ORDER BY day)
        TO '{impressions_by_day_path}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD');
    """)
    
    publisher_country_day_path = out_dir / "publisher_country_day_revenue.parquet"
    con.execute(f"""
        COPY (SELECT publisher_id, country, day, SUM(bid_price) AS sum_bid_price
              FROM events
              WHERE type = 'impression'
              GROUP BY publisher_id, country, day)
        TO '{publisher_country_day_path}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD');
    """)
    
    minute_revenue_path = out_dir / "minute_impression_revenue.parquet"
    con.execute(f"""
        COPY (SELECT minute, SUM(bid_price) AS sum_bid_price
              FROM events
              WHERE type = 'impression'
              GROUP BY minute
              ORDER BY minute)
        TO '{minute_revenue_path}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD');
    """)
    
    print(f"✅ All aggregations computed in {time.time() - t0:.2f}s")
    
    con.close()
    print("🟩 Prepare phase completed")

# -------------------
# Run Queries
# -------------------
def run_phase(query_list, prepared_dir: Path, out_dir: Path):
    """
    Run phase – Once queries are provided, your solution must execute them and output correct results. No outbound network access should happen during this phase.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    
    con = duckdb.connect()
    
    # Optimize for query execution
    con.execute("SET threads TO 8;")
    con.execute("SET memory_limit = '12GB';")
    
    partitioned_path = str(prepared_dir / "events_partitioned")
    
    print(f"🟩 Loading partitioned data from {partitioned_path}")
    con.execute(f"""
        CREATE VIEW events AS 
        SELECT * FROM read_parquet('{partitioned_path}/**/*.parquet', hive_partitioning=1);
    """)
    
    # Load paths to pre-computed aggregation parquet files
    adv_counts_path = str(prepared_dir / "adv_type_counts.parquet")
    purchase_stats_path = str(prepared_dir / "purchase_country_stats.parquet")
    impression_day_path = str(prepared_dir / "impression_day_revenue.parquet")
    publisher_country_day_path = str(prepared_dir / "publisher_country_day_revenue.parquet")
    minute_revenue_path = str(prepared_dir / "minute_impression_revenue.parquet")
    
    # Create materialized views
    con.execute(f"CREATE VIEW adv_counts AS SELECT * FROM '{adv_counts_path}';")
    con.execute(f"CREATE VIEW purchase_stats AS SELECT * FROM '{purchase_stats_path}';")
    con.execute(f"CREATE VIEW impression_day AS SELECT * FROM '{impression_day_path}';")
    con.execute(f"CREATE VIEW publisher_country_day AS SELECT * FROM '{publisher_country_day_path}';")
    con.execute(f"CREATE VIEW minute_revenue AS SELECT * FROM '{minute_revenue_path}';")
    
    results = []
    for i, query in enumerate(query_list, start=1):
        print(f"\n🟦 Query {i}:\n{query}\n")
        t0 = time.time()
        
        sql = build_optimized_sql(query) # Routes to query planner to build SQL query
        
        res = con.execute(sql)
        rows = res.fetchall()
        cols = [d[0] for d in res.description]
        
        dt = time.time() - t0
        print(f"✅ Rows: {len(rows)} | Time: {dt:.3f}s")
        
        out_path = out_dir / f"q{i}.csv"
        with out_path.open("w", newline="") as f:
            import csv
            w = csv.writer(f)
            w.writerow(cols)
            w.writerows(rows)
        
        results.append({"query": i, "rows": len(rows), "time": dt})
    
    con.close()
    
    print("\nSummary:")
    for r in results:
        print(f"Q{r['query']}: {r['time']:.3f}s ({r['rows']} rows)")
    print(f"Total time: {sum(r['time'] for r in results):.3f}s")
    
    return results


def build_optimized_sql(query):
    """
    Build optimized SQL via query rewriting using view selection. This enables order of magnitudes speedup.
    """

    # Daily impression revenue lookup, referencing inputs.py queries[0]
    if (query.get("group_by") == ["day"] and 
        any(isinstance(s, dict) and s.get("SUM") == "bid_price" for s in query["select"]) and
        query.get("where") == [{"col": "type", "op": "eq", "val": "impression"}]):
        return "SELECT day, sum_bid_price AS \"SUM(bid_price)\" FROM impression_day ORDER BY day"
    
    # (publisher, country, day) impression revenue lookup, referencing inputs.py queries[1]
    if (query.get("group_by") == ["publisher_id"] and
        any(isinstance(s, dict) and s.get("SUM") == "bid_price" for s in query["select"])):
        where_conditions = query.get("where", [])
        has_impression = any(c.get("col") == "type" and c.get("val") == "impression" for c in where_conditions)
        country_filter = next((c for c in where_conditions if c.get("col") == "country"), None)
        day_filter = next((c for c in where_conditions if c.get("col") == "day"), None)
        
        if has_impression and country_filter and day_filter:
            country_val = country_filter.get("val")
            day_op = day_filter.get("op")
            day_val = day_filter.get("val")
            
            if day_op == "between":
                low_day, high_day = day_val
                return f"""
                    SELECT publisher_id, SUM(sum_bid_price) AS "SUM(bid_price)"
                    FROM publisher_country_day
                    WHERE country = '{country_val}' 
                      AND day BETWEEN '{low_day}' AND '{high_day}'
                    GROUP BY publisher_id
                """
            elif day_op == "eq":
                return f"""
                    SELECT publisher_id, SUM(sum_bid_price) AS "SUM(bid_price)"
                    FROM publisher_country_day
                    WHERE country = '{country_val}' AND day = '{day_val}'
                    GROUP BY publisher_id
                """
    
    # Derive AVG from SUM and COUNT, referencing inputs.py queries[2]
    if (query.get("group_by") == ["country"] and 
        any(isinstance(s, dict) and s.get("AVG") == "total_price" for s in query["select"]) and
        len(query.get("where", [])) == 1 and
        query.get("where")[0].get("col") == "type" and
        query.get("where")[0].get("val") == "purchase"):
        order_clause = ""
        if query.get("order_by"):
            for o in query["order_by"]:
                if "AVG" in o["col"]:
                    direction = o["dir"].upper()
                    order_clause = f"ORDER BY \"AVG(total_price)\" {direction}"
        return f"""
            SELECT country, 
                   (total_revenue / purchase_count) AS "AVG(total_price)"
            FROM purchase_stats
            {order_clause}
        """
    # Advertiser event counts by type, referencing inputs.py queries[3] and order by descending count
    if (query.get("group_by") == ["advertiser_id", "type"] and 
        any(isinstance(s, dict) and s.get("COUNT") == "*" for s in query["select"]) and
        not query.get("where")):
        order_clause = ""
        if query.get("order_by"):
            for o in query["order_by"]:
                if "COUNT" in o["col"]:
                    direction = o["dir"].upper()
                    order_clause = f"ORDER BY count_star {direction}"
        return f"""
            SELECT advertiser_id, type, count_star AS "COUNT(*)"
            FROM adv_counts
            {order_clause}
        """
    
    # Minute-level impression revenue lookup, referencing inputs.py queries[4]
    if (query.get("group_by") == ["minute"] and
        any(isinstance(s, dict) and s.get("SUM") == "bid_price" for s in query["select"])):
        where_conditions = query.get("where", [])
        has_impression = any(c.get("col") == "type" and c.get("val") == "impression" for c in where_conditions)
        day_filter = next((c for c in where_conditions if c.get("col") == "day"), None)
        
        if has_impression and day_filter:
            day_op = day_filter.get("op")
            day_val = day_filter.get("val")
            
            if day_op == "eq":
                order_clause = ""
                if query.get("order_by"):
                    for o in query["order_by"]:
                        if o["col"] == "minute":
                            direction = o["dir"].upper()
                            order_clause = f"ORDER BY minute {direction}"
                
                return f"""
                    SELECT minute, sum_bid_price AS "SUM(bid_price)"
                    FROM minute_revenue
                    WHERE minute LIKE '{day_val}%'
                    {order_clause}
                """
    
    # Default if no query planning is needed
    return assemble_sql(query)

# -------------------
# Main Entry Point
# -------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Querry Planner Challenge — runs benchmark queries on input CSV data."
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        required=True,
        help="The folder where the input CSV is provided"
    )
    parser.add_argument(
        "--prepared-dir",
        type=Path,
        required=True,
        help="The folder where the pre-aggregated Parquet data is stored"
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        required=True,
        help="Where to output query results"
    )
    parser.add_argument(
        "--prepare",
        action="store_true",
        help="Run the prepare phase"
    )
    parser.add_argument(
        "--run",
        action="store_true",
        help="Run the query execution phase"
    )
    
    args = parser.parse_args()
    
    if args.prepare:
        print("=" * 60)
        print("Prepare phase")
        print("=" * 60)
        t0 = time.time()
        prepare_phase(args.data_dir, args.prepared_dir)
        dt = time.time() - t0
        print(f"\n✅ Prepare phase completed in {dt:.3f}s")
    
    if args.run:
        print("\n" + "=" * 60)
        print("Run phase")
        print("=" * 60)
        run_phase(queries, args.prepared_dir, args.out_dir)