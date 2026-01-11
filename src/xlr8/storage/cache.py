"""
Cache management for XLR8 Parquet storage.

This module provides query-specific caching for MongoDB results:

1. Query Hashing (hash_query):
   - Creates deterministic MD5 hash from query parameters (filter, projection, sort)
   - Normalizes datetimes to ISO format, ObjectIds to strings
   - Recursively sorts dicts for determinism
   - Same query always produces same hash

2. Cache Lifecycle (CacheManager):
   - Each query gets unique directory: .cache/{query_hash}/
   - Manages Parquet file storage per query
   - Provides cache existence checking, file listing, cleanup

Usage:
    # Hash a query
    query_hash = hash_query(filter_dict={"timestamp": {"$gte": start_date}})

    # Manage cache lifecycle
    cache = CacheManager(filter_dict={"timestamp": {"$gte": start_date}})
    cache.ensure_cache_dir()
    # ... write parquet files to cache.cache_dir ...
    if cache.exists():
        files = cache.list_parquet_files()
    cache.clean()  # Remove when done
"""

import hashlib
import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from bson import ObjectId


def hash_query(
    filter_dict: Dict[str, Any],
    projection: Optional[Dict[str, Any]] = None,
    sort: Optional[list] = None,
) -> str:
    """
    Create deterministic hash of query parameters.

    Uses MD5 hash of canonicalized JSON to create unique cache directory name.
    Same query parameters will always produce the same hash.

    Args:
        filter_dict: MongoDB filter dictionary
        projection: Field projection
        sort: Sort specification

    Returns:
        Hex string hash (32 characters)

    Example:
        >>> hash_query({"timestamp": {"$gte": "2024-01-01"}})
        'a3f5c9d2e1b4f6a8c7e9d1b3f5a7c9e1'
    """

    def normalize_value(obj):
        """
        Recursively normalize query values for deterministic hashing.

        Converts datetimes to ISO strings, ObjectIds to strings,
        and sorts dict keys to ensure same query always hashes identically.
        """
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, ObjectId):
            return str(obj)
        elif isinstance(obj, dict):
            return {k: normalize_value(v) for k, v in sorted(obj.items())}
        elif isinstance(obj, list):
            return [normalize_value(v) for v in obj]
        return obj

    # Build canonical representation
    query_repr = {
        "filter": normalize_value(filter_dict),
    }

    if projection:
        query_repr["projection"] = normalize_value(projection)

    if sort:
        query_repr["sort"] = normalize_value(sort)

    # Create deterministic JSON (sorted keys)
    json_str = json.dumps(query_repr, sort_keys=True, separators=(",", ":"))

    # Hash it
    return hashlib.md5(json_str.encode("utf-8")).hexdigest()


class CacheManager:
    """
    Manages Parquet cache lifecycle for a specific query.

    Each query gets a unique cache directory based on query hash:
    .cache/{query_hash}/

    Provides:
    - Cache directory creation
    - Cache existence checking
    - Cache cleanup

    Example:
        >>> cache = CacheManager(filter_dict={"timestamp": {"$gte": start}})
        >>> cache.ensure_cache_dir()
        >>> # ... write parquet files to cache.cache_dir ...
        >>> cache.clean()  # Remove cache when done
    """

    def __init__(
        self,
        filter_dict: Dict[str, Any],
        projection: Optional[Dict[str, Any]] = None,
        sort: Optional[list] = None,
        cache_root: Path = Path(".cache"),
    ):
        """
        Initialize cache manager for a query.

        Args:
            filter_dict: MongoDB filter
            projection: Field projection
            sort: Sort specification
            cache_root: Root directory for all caches (default: .cache)
        """
        self.filter_dict = filter_dict
        self.projection = projection
        self.sort = sort
        self.cache_root = Path(cache_root)

        # Generate query hash
        self.query_hash = hash_query(filter_dict, projection, sort)

        # Cache directory for this specific query
        self.cache_dir = self.cache_root / self.query_hash

    def ensure_cache_dir(self) -> Path:
        """
        Create cache directory if it doesn't exist.

        Returns:
            Path to cache directory
        """
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        return self.cache_dir

    def exists(self) -> bool:
        """
        Check if cache directory exists and has parquet files.

        Returns:
            True if cache exists with .parquet files
        """
        if not self.cache_dir.exists():
            return False

        # Check for at least one parquet file
        parquet_files = list(self.cache_dir.glob("*.parquet"))
        return len(parquet_files) > 0

    def list_parquet_files(self) -> list[Path]:
        """
        List all parquet files in cache directory.

        Returns:
            List of parquet file paths, sorted by name
        """
        if not self.cache_dir.exists():
            return []

        files = sorted(self.cache_dir.glob("*.parquet"))
        return files

    def clean(self) -> bool:
        """
        Remove cache directory and all contents.

        Use after downloading data to free disk space.

        Returns:
            True if cache was removed, False if didn't exist
        """
        if not self.cache_dir.exists():
            return False

        shutil.rmtree(self.cache_dir)
        return True

    def get_metadata(self) -> Dict[str, Any]:
        """
        Get cache metadata.

        Returns:
            Dict with keys:
                - query_hash (str): Full hash of the query
                - cache_dir (str): Path to cache directory
                - exists (bool): Whether cache has parquet files
                - file_count (int): Number of parquet files
                - total_size_mb (float): Total size in megabytes
        """
        parquet_files = self.list_parquet_files()

        total_size = sum(f.stat().st_size for f in parquet_files)
        total_size_mb = total_size / (1024 * 1024)

        return {
            "query_hash": self.query_hash,
            "cache_dir": str(self.cache_dir),
            "exists": self.exists(),
            "file_count": len(parquet_files),
            "total_size_mb": round(total_size_mb, 2),
        }

    def __repr__(self) -> str:
        meta = self.get_metadata()
        return (
            f"CacheManager(hash={self.query_hash[:8]}..., "
            f"exists={meta['exists']}, files={meta['file_count']}, "
            f"size={meta['total_size_mb']:.1f}MB)"
        )
