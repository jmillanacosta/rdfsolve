"""Test metadata queries on all available endpoints."""
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import rdfsolve
from rdfsolve.endpoint_health import check_endpoint_health

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Suppress verbose logs from dependencies
logging.getLogger("rdfsolve.sparql_helper").setLevel(logging.WARNING)
logging.getLogger("rdfsolve.miner").setLevel(logging.WARNING)
logging.getLogger("rdfsolve.metadata").setLevel(logging.DEBUG)


def test_endpoint_metadata(endpoint_url: str, name: str, timeout: int = 15) -> dict:
    """Test metadata capture for a single endpoint.

    Args:
        endpoint_url: SPARQL endpoint URL
        name: Dataset name
        timeout: Query timeout in seconds

    Returns:
        dict with metadata test results
    """
    result = {
        "endpoint": endpoint_url,
        "name": name,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "status": "unknown",
        "metadata": {},
        "error": None,
    }

    try:
        logger.info(f"Testing: {name}")

        # Quick health check with short timeout (5s)
        health = check_endpoint_health(endpoint_url, timeout=5)
        if health.status != "up":
            result["status"] = "down"
            result["error"] = health.error_message or "Endpoint not responding"
            logger.warning(f"  ✗ Down: {result['error'][:60]}")
            return result

        response_time_ms = (health.response_time * 1000) if health.response_time else 0
        logger.info(f"  ✓ Up ({response_time_ms:.0f}ms)")

        # Query metadata using public API
        metadata = rdfsolve.query_metadata(endpoint_url, timeout=float(timeout))

        if metadata:
            result["status"] = "success"
            result["metadata"] = metadata

            logger.info(f"  ✓ Metadata: {list(metadata.keys())}")

            # Log creator count
            creators = metadata.get("source_creator", [])
            if creators:
                logger.info(f"    - {len(creators)} creator(s)")
                for creator in creators[:3]:  # Show first 3
                    preview = creator[:60] + "..." if len(creator) > 60 else creator
                    logger.info(f"      • {preview}")
                if len(creators) > 3:
                    logger.info(f"      ... and {len(creators) - 3} more")

            # Log other key fields
            for key in ["source_license", "source_version", "source_issued", "title"]:
                if key in metadata:
                    val = str(metadata[key])
                    preview = val[:60] + "..." if len(val) > 60 else val
                    logger.info(f"    - {key}: {preview}")

        else:
            result["status"] = "no_metadata"
            logger.info("  ⚠ No metadata found")

    except Exception as e:
        result["status"] = "error"
        result["error"] = str(e)
        logger.error(f"  ✗ Error: {str(e)[:80]}")

    return result


def main():
    """Test metadata queries on all known endpoints."""
    # Load sources from rdfsolve
    logger.info("Loading source endpoints...")
    sources = rdfsolve.load_sources()
    logger.info(f"Found {len(sources)} source entries")

    # Filter to endpoints that have a SPARQL endpoint URL
    endpoints = [
        (s["name"], s["endpoint"])
        for s in sources
        if s.get("endpoint") and "sparql" in s.get("endpoint", "").lower()
    ]

    logger.info(f"Testing {len(endpoints)} SPARQL endpoints for metadata...\n")

    # Test endpoints with some parallelism (but not too much to avoid hammering)
    results = []
    max_workers = 5  # Test 5 endpoints at a time

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_endpoint = {
            executor.submit(test_endpoint_metadata, url, name, timeout=15): (name, url)
            for name, url in endpoints
        }

        # Process results as they complete
        for future in as_completed(future_to_endpoint):
            name, url = future_to_endpoint[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                logger.error(f"Failed to test {name}: {e}")
                results.append({
                    "endpoint": url,
                    "name": name,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "status": "error",
                    "metadata": {},
                    "error": str(e),
                })

    # Summary report
    logger.info(f"\n{'='*80}")
    logger.info("SUMMARY REPORT")
    logger.info(f"{'='*80}")

    success_count = sum(1 for r in results if r["status"] == "success")
    down_count = sum(1 for r in results if r["status"] == "down")
    no_metadata_count = sum(1 for r in results if r["status"] == "no_metadata")
    error_count = sum(1 for r in results if r["status"] == "error")
    with_metadata = sum(1 for r in results if r.get("metadata"))

    up_count = success_count + no_metadata_count

    logger.info(f"Total endpoints tested: {len(results)}")
    logger.info(f"Up: {up_count}")
    logger.info(f"Down: {down_count}")
    logger.info(f"Errors: {error_count}")
    logger.info(f"With metadata: {with_metadata}/{up_count} ({with_metadata/up_count*100 if up_count else 0:.1f}%)")

    # Metadata field statistics
    logger.info(f"\n{'='*80}")
    logger.info("METADATA FIELDS FOUND")
    logger.info(f"{'='*80}")

    field_counts = {}
    for result in results:
        if result.get("metadata"):
            for field in result["metadata"]:
                field_counts[field] = field_counts.get(field, 0) + 1

    for field, count in sorted(field_counts.items(), key=lambda x: x[1], reverse=True):
        logger.info(f"  {field}: {count} endpoints")

    # Endpoints with metadata
    logger.info(f"\n{'='*80}")
    logger.info(f"ENDPOINTS WITH METADATA ({with_metadata} total)")
    logger.info(f"{'='*80}")
    for result in results:
        if result.get("metadata"):
            logger.info(f"\n{result['name']}:")
            logger.info(f"  Endpoint: {result['endpoint']}")
            metadata = result['metadata']

            # Show creators
            creators = metadata.get('source_creator', [])
            if creators:
                logger.info(f"  Creators ({len(creators)}):")
                for creator in creators:
                    logger.info(f"    • {creator}")

            # Show other fields
            for key, value in metadata.items():
                if key == 'source_creator':
                    continue  # Already shown
                if isinstance(value, list):
                    logger.info(f"  {key}: {value}")
                else:
                    preview = str(value)[:80]
                    logger.info(f"  {key}: {preview}")

    # Save detailed results
    output_file = Path("metadata_test_results.json")
    with output_file.open("w") as f:
        json.dump(results, f, indent=2)
    logger.info(f"\nDetailed results saved to: {output_file}")


if __name__ == "__main__":
    main()
