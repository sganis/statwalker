#!/usr/bin/env python3
"""
Redis-based filesystem index migration from in-memory Rust implementation.

This script provides:
1. CSV ingestion into Redis with optimized data structures
2. Query functions equivalent to Rust's list_children and get_item
3. Memory-efficient storage using Redis hashes and sets
"""

import csv
import json
import redis
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, asdict
from pathlib import Path
import argparse
import time


@dataclass
class Age:
    count: int
    disk: int # bytes
    atime: int # unix timestamp
    mtime: int # unix timestamp


@dataclass
class FolderOut:
    path: str
    users: Dict[str, Dict[str, Age]] # username -> age_string -> stats


@dataclass
class Stats:
    file_count: int = 0
    disk_bytes: int = 0
    latest_atime: int = 0
    latest_mtime: int = 0


class RedisFileSystemIndex:
    """
    Redis-based filesystem index with the following key design:
    
    1. Path structure: "path_structure:<path>" -> Set of child directory names
    2. Path users: "path_users:<path>" -> Set of usernames that have data under this path
    3. Aggregated data: "stats:<path>:<user>:<age>" -> Hash with {count, disk, atime, mtime}
    4. All users: "all_users" -> Set of all usernames
    5. Path hierarchy: "path_children:<path>" -> Set of immediate children paths
    
    This design optimizes for:
    - Fast path traversal (O(1) for finding children)
    - Efficient user filtering (set operations)
    - Memory efficiency (Redis hashes for structured data)
    - Range queries by age (separate keys per age bucket)
    """
    
    def __init__(self, redis_client: redis.Redis, key_prefix: str = "fs_index"):
        self.redis = redis_client
        self.prefix = key_prefix
        self.total_entries = 0
        
    def _key(self, key_type: str, *args) -> str:
        """Generate Redis key with consistent prefixing."""
        parts = [self.prefix, key_type] + [str(arg) for arg in args]
        return ":".join(parts)
    
    def _canonical_path(self, path: str) -> str:
        """Normalize path similar to Rust implementation."""
        normalized = path.replace('\\', '/')
        if not normalized.startswith('/'):
            normalized = '/' + normalized
        if len(normalized) > 1:
            normalized = normalized.rstrip('/')
        return normalized
    
    def _path_to_components(self, path: str) -> List[str]:
        """Split path into components."""
        canonical = self._canonical_path(path)
        return [c for c in canonical.split('/') if c]
    
    def _parent_path(self, path: str) -> str:
        """Get parent path."""
        canonical = self._canonical_path(path)
        if canonical == '/':
            return None
        components = self._path_to_components(canonical)
        if not components:
            return '/'
        return '/' + '/'.join(components[:-1]) if len(components) > 1 else '/'
    
    def clear_all(self):
        """Clear all filesystem index data from Redis."""
        pattern = f"{self.prefix}:*"
        keys = self.redis.keys(pattern)
        if keys:
            self.redis.delete(*keys)
            print(f"Cleared {len(keys)} Redis keys")
    
    def load_from_csv(self, csv_path: str, clear_existing: bool = True) -> List[str]:
        """
        Load CSV data into Redis with progress tracking.
        
        CSV format: path,user,age,files,disk,accessed,modified
        """
        if clear_existing:
            self.clear_all()
        
        # Count total lines for progress
        print(f"Counting lines in {csv_path}...")
        with open(csv_path, 'r') as f:
            total_lines = sum(1 for _ in f) - 1 # subtract header
        
        print(f"Total data lines: {total_lines}")
        print("Loading and building Redis index...")
        
        all_users = set()
        loaded_count = 0
        progress_interval = max(total_lines // 10, 1)
        
        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f)
            
            # Use Redis pipeline for batch operations
            pipe = self.redis.pipeline()
            batch_size = 1000
            batch_count = 0
            
            for row_num, row in enumerate(reader):
                try:
                    path_str = row['path'].strip()
                    username = row['user'].strip()
                    age = int(row['age'])
                    file_count = int(row['files'])
                    disk_bytes = int(row['disk'])
                    latest_atime = int(row['accessed'])
                    latest_mtime = int(row['modified'])
                    
                    if not path_str or not username:
                        continue
                    
                    canonical_path = self._canonical_path(path_str)
                    all_users.add(username)
                    
                    # Store aggregated stats
                    stats_key = self._key("stats", canonical_path, username, age)
                    pipe.hset(stats_key, mapping={
                        'count': file_count,
                        'disk': disk_bytes,
                        'atime': latest_atime,
                        'mtime': latest_mtime
                    })
                    
                    # Track users per path
                    users_key = self._key("path_users", canonical_path)
                    pipe.sadd(users_key, username)
                    
                    # Build path hierarchy
                    self._add_to_hierarchy(pipe, canonical_path)
                    
                    batch_count += 1
                    loaded_count += 1
                    
                    # Execute batch
                    if batch_count >= batch_size:
                        pipe.execute()
                        pipe = self.redis.pipeline()
                        batch_count = 0
                    
                    # Progress reporting
                    if (row_num + 1) % progress_interval == 0:
                        percent = min(100, int((row_num + 1) * 100 / total_lines))
                        print(f"{percent}%")
                        
                except (ValueError, KeyError) as e:
                    print(f"Skipping invalid row {row_num + 2}: {e}")
                    continue
            
            # Execute remaining batch
            if batch_count > 0:
                pipe.execute()
        
        # Store all users set
        if all_users:
            self.redis.delete(self._key("all_users"))
            self.redis.sadd(self._key("all_users"), *all_users)
        
        self.total_entries = loaded_count
        users_list = sorted(list(all_users))
        
        print(f"Loaded {loaded_count} entries for {len(users_list)} users")
        return users_list
    
    def _add_to_hierarchy(self, pipe: redis.client.Pipeline, path: str):
        """Add path to hierarchy structure."""
        canonical = self._canonical_path(path)
        parent = self._parent_path(canonical)
        
        if parent is not None:
            # Add this path as child of parent
            children_key = self._key("path_children", parent)
            path_name = canonical.split('/')[-1] if canonical != '/' else ''
            if path_name:
                pipe.sadd(children_key, path_name)
            
            # Recursively ensure parent hierarchy exists
            self._add_to_hierarchy(pipe, parent)
    
    def get_all_users(self) -> List[str]:
        """Get all users in the system."""
        users = self.redis.smembers(self._key("all_users"))
        return sorted([u.decode('utf-8') for u in users])
    
    def list_children(self, dir_path: str, user_filter: List[str] = None, 
                    age_filter: Optional[int] = None) -> List[FolderOut]:
        """
        List child directories with aggregated stats.
        
        Args:
            dir_path: Directory path to list
            user_filter: List of usernames to filter by (empty = all users)
            age_filter: Age bucket to filter by (0, 1, 2, or None for all)
        
        Returns:
            List of FolderOut objects with aggregated data
        """
        canonical_path = self._canonical_path(dir_path)
        
        # Get child directory names
        children_key = self._key("path_children", canonical_path)
        child_names = self.redis.smembers(children_key)
        
        if not child_names:
            return []
        
        items = []
        
        for child_name_bytes in child_names:
            child_name = child_name_bytes.decode('utf-8')
            
            # Construct full path
            if canonical_path == '/':
                full_path = f"/{child_name}"
            else:
                full_path = f"{canonical_path}/{child_name}"
            
            # Get users for this path
            users_key = self._key("path_users", full_path)
            available_users = self.redis.smembers(users_key)
            available_users = {u.decode('utf-8') for u in available_users}
            
            if not available_users:
                continue
            
            # Apply user filter
            if user_filter:
                users_to_show = available_users.intersection(set(user_filter))
            else:
                users_to_show = available_users
            
            if not users_to_show:
                continue
            
            # Build user -> age -> stats mapping
            users_map = {}
            ages_to_consider = [age_filter] if age_filter is not None else [0, 1, 2]
            
            for username in sorted(users_to_show):
                age_map = {}
                
                for age in ages_to_consider:
                    stats_key = self._key("stats", full_path, username, age)
                    stats_data = self.redis.hgetall(stats_key)
                    
                    if stats_data:
                        age_map[str(age)] = Age(
                            count=int(stats_data[b'count']),
                            disk=int(stats_data[b'disk']),
                            atime=int(stats_data[b'atime']),
                            mtime=int(stats_data[b'mtime'])
                        )
                
                if age_map:
                    users_map[username] = age_map
            
            if users_map:
                items.append(FolderOut(path=full_path, users=users_map))
        
        # Sort by path
        items.sort(key=lambda x: x.path)
        return items
    
    def get_item(self, path: str, username: str, age: int) -> Optional[Age]:
        """
        Get specific item stats for a path, user, and age.
        
        Args:
            path: File system path
            username: Username
            age: Age bucket (0, 1, or 2)
            
        Returns:
            Age object with stats or None if not found
        """
        canonical_path = self._canonical_path(path)
        stats_key = self._key("stats", canonical_path, username, age)
        stats_data = self.redis.hgetall(stats_key)
        
        if not stats_data:
            return None
        
        return Age(
            count=int(stats_data[b'count']),
            disk=int(stats_data[b'disk']),
            atime=int(stats_data[b'atime']),
            mtime=int(stats_data[b'mtime'])
        )
    
    def get_path_users(self, path: str) -> Set[str]:
        """Get all users that have data under the given path."""
        canonical_path = self._canonical_path(path)
        users_key = self._key("path_users", canonical_path)
        users = self.redis.smembers(users_key)
        return {u.decode('utf-8') for u in users}
    
    def get_stats(self) -> Dict:
        """Get index statistics."""
        all_keys = self.redis.keys(f"{self.prefix}:*")
        stats_keys = [k for k in all_keys if b":stats:" in k]
        
        used_bytes = 0
        try:
            info = self.redis.info(section="memory")  # returns dict[str, Any]
            used_bytes = int(info.get("used_memory", 0))
        except Exception:
            pass

        return {
            "total_redis_keys": len(all_keys),
            "stats_entries": len(stats_keys),
            "total_users": self.redis.scard(self._key("all_users")),
            "used_memory_bytes": used_bytes,
            "used_memory_mb": round(used_bytes / 1024 / 1024, 2),
        }


def pretty_print_results(items: List[FolderOut]):
    """Pretty print query results."""
    if not items:
        print("No results found")
        return
    
    for item in items:
        print(f"\nPath: {item.path}")
        for username, age_data in item.users.items():
            print(f" User: {username}")
            for age_str, age_obj in age_data.items():
                print(f"   Age {age_str}: {age_obj.count} files, {age_obj.disk:,} bytes")


def main():
    parser = argparse.ArgumentParser(description="Redis Filesystem Index")
    parser.add_argument("command", choices=["load", "query", "stats"], help="Command to execute")
    parser.add_argument("--csv", help="CSV file path for loading")
    parser.add_argument("--path", default="/", help="Path to query")
    parser.add_argument("--users", help="Comma-separated users to filter")
    parser.add_argument("--age", type=int, choices=[0, 1, 2], help="Age filter")
    parser.add_argument("--redis-host", default="localhost", help="Redis host")
    parser.add_argument("--redis-port", type=int, default=6379, help="Redis port")
    parser.add_argument("--redis-db", type=int, default=0, help="Redis database")
    parser.add_argument("--clear", action="store_true", help="Clear existing data")
    
    args = parser.parse_args()
    
    # Connect to Redis
    r = redis.Redis(host=args.redis_host, port=args.redis_port, 
                    db=args.redis_db, decode_responses=False)
    
    try:
        r.ping()
        print(f"Connected to Redis at {args.redis_host}:{args.redis_port}")
    except redis.ConnectionError:
        print(f"Failed to connect to Redis at {args.redis_host}:{args.redis_port}")
        return 1
    
    index = RedisFileSystemIndex(r)
    
    if args.command == "load":
        if not args.csv:
            print("--csv required for load command")
            return 1
        
        start_time = time.time()
        users = index.load_from_csv(args.csv, clear_existing=args.clear)
        elapsed = time.time() - start_time
        
        print(f"\nLoad completed in {elapsed:.2f} seconds")
        print(f"Users: {len(users)}")
        print("Index statistics:", index.get_stats())
        
    elif args.command == "query":
        user_filter = []
        if args.users:
            user_filter = [u.strip() for u in args.users.split(',') if u.strip()]
        
        print(f"Querying path: {args.path}")
        if user_filter:
            print(f"User filter: {user_filter}")
        if args.age is not None:
            print(f"Age filter: {args.age}")
        
        results = index.list_children(args.path, user_filter, args.age)
        pretty_print_results(results)
        
    elif args.command == "stats":
        stats = index.get_stats()
        print("Redis Index Statistics:")
        for key, value in stats.items():
            print(f" {key}: {value}")
        
        print(f" All users: {index.get_all_users()}")
    
    return 0


if __name__ == "__main__":
    exit(main())

if __name__ == "__main__" and False: # Set to True to run examples
    
    # Example 1: Load data
    r = redis.Redis(host='localhost', port=6379, db=0)  
    index = RedisFileSystemIndex(r)
    
    # Load from CSV
    users = index.load_from_csv("your_data.csv")
    print(f"Loaded data for users: {users}")
    
    # Example 2: Query all children of root
    results = index.list_children("/")
    pretty_print_results(results)
    
    # Example 3: Query with filters
    results = index.list_children("/", user_filter=["alice", "bob"], age_filter=0)
    pretty_print_results(results)
    
    # Example 4: Get specific item
    age_obj = index.get_item("/some/path", "alice", 1)
    if age_obj:
        print(f"Alice's age-1 data: {age_obj.count} files, {age_obj.disk} bytes")
    
    # Example 5: Get statistics
    stats = index.get_stats()
    print("Statistics:", stats)
