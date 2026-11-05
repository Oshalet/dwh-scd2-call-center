"""Test data generator for call_center_requests."""

import random
import sys
from datetime import datetime, timedelta
import argparse

from scripts.db.postgres import execute, execute_many, fetch_all

SEED = 42

STATUSES_FLOW = {
    "new": ["in_progress"],
    "in_progress": ["closed", "rejected"],
    "closed": [],
    "rejected": [],
}

def generate_initial_snapshot(n_requests: int = 100, days_back: int = 30) -> int:
    """Insert initial snapshot: status 'new', created_at spread over past days."""
    random.seed(SEED)
    rows = []
    for _ in range(n_requests):
        created = datetime.now() - timedelta(days=random.uniform(0, days_back))
        phone = f"+7{random.randint(1000000000, 9999999999)}"
        rows.append((phone, random.randint(1, 10), "new", created, created))
    return execute_many(
        """INSERT INTO call_center_requests
               (client_phone, operator_id, status, created_at, updated_at)
           VALUES (%s, %s, %s, %s, %s)""",
        rows,
    )
    
def generate_changes(change_ratio: float = 0.2) -> int:
    """Advance a random subset of non-terminal requests to the next status."""
    random.seed(SEED)
    
    current = fetch_all("SELECT id, status FROM call_center_requests")
    candidates = [r for r in current if STATUSES_FLOW[r['status']]]
    n_to_change = int(len(candidates) * change_ratio)
    to_change = random.sample(candidates, n_to_change)
    
    updates = []
    for row in to_change:
        next_status = random.choice(STATUSES_FLOW[row["status"]])
        updates.append((next_status, row["id"]))
        
    if not updates:
        return 0
    return execute_many("UPDATE call_center_requests SET status = %s WHERE id = %s",
                        updates
                        )
        
def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Generate test data for call center requests.")
    subparser = parser.add_subparsers(dest="command",required=True)
    
    init_parser = subparser.add_parser("init", help="Insert initial snapshot of requests.")
    init_parser.add_argument("--n", type=int, default=100, help="Number of requests (default: 100)")
    init_parser.add_argument("--days", type=int, default=30, help="Spread in days back (default: 30)")
    
    subparser.add_parser("changes", help="Advance statuses of a subset of requests.")
    
    args = parser.parse_args()
    
    if args.command == "init":
        n = generate_initial_snapshot(n_requests=args.n, days_back=args.days)
        print(f"Initial snapshot: inserted {n} rows.")
    elif args.command == "changes":
        n = generate_changes()
        print(f"Changes applied to {n} rows.")
        
if __name__ == "__main__":
    main()