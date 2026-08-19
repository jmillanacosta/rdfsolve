#!/bin/bash
# Git History Reorganization Script for RDFSolve
# This script consolidates 270 commits into ~20 meaningful commits
# and removes transient features (Flask backend, Docker, MCP submodule)

set -e

echo "=== RDFSolve Git History Reorganization ==="
echo ""
echo "This script will:"
echo "  - Create a backup of your current branch"
echo "  - Remove Flask backend, Docker, and MCP submodule from history"
echo "  - Consolidate 270 commits into 19 meaningful commits"
echo ""
echo "WARNING: This rewrites git history and cannot be easily undone!"
echo "Make sure you have pushed to a backup remote or have another backup."
echo ""

# Check if we're in a git repo
if ! git rev-parse --git-dir > /dev/null 2>&1; then
    echo "ERROR: Not in a git repository"
    exit 1
fi

# Check for uncommitted changes
if ! git diff-index --quiet HEAD --; then
    echo "ERROR: You have uncommitted changes. Please commit or stash them first."
    git status --short
    exit 1
fi

CURRENT_BRANCH=$(git branch --show-current)
TOTAL_COMMITS=$(git log --oneline | wc -l)

echo "Current branch: $CURRENT_BRANCH"
echo "Total commits: $TOTAL_COMMITS"
echo ""
read -p "Do you want to proceed? Type 'yes' to continue: " CONFIRM

if [ "$CONFIRM" != "yes" ]; then
    echo "Aborted."
    exit 0
fi

# Create backup branch
BACKUP_BRANCH="backup-original-history-$(date +%Y%m%d-%H%M%S)"
echo ""
echo "Creating backup branch: $BACKUP_BRANCH"
git branch "$BACKUP_BRANCH"
echo "✓ Backup created"

# Step 1: Remove transient directories using git-filter-repo (if available)
echo ""
echo "Step 1: Checking for git-filter-repo..."

if command -v git-filter-repo &> /dev/null; then
    echo "Found git-filter-repo. Removing transient features from history..."

    # Create a fresh clone to work with (filter-repo requirement)
    TEMP_DIR=$(mktemp -d)
    echo "Cloning repository to temporary directory: $TEMP_DIR"
    git clone --no-local . "$TEMP_DIR"
    cd "$TEMP_DIR"

    # Remove Flask backend
    if [ -d "src/rdfsolve/backend" ]; then
        echo "  Removing Flask backend..."
        git filter-repo --path src/rdfsolve/backend --invert-paths --force
    fi

    # Remove Docker files
    echo "  Removing Docker files..."
    git filter-repo --path docker-compose.yml --path docker-compose.full.yml \
                    --path docker-compose.pipeline.yml --path Dockerfile \
                    --invert-paths --force 2>/dev/null || true

    # Remove MCP submodule
    if [ -d "mcp" ]; then
        echo "  Removing MCP submodule..."
        git filter-repo --path mcp --invert-paths --force
    fi

    echo "✓ Transient features removed"

    # Go back to original directory
    cd -

    # Add filtered repo as remote and fetch
    git remote add filtered "$TEMP_DIR"
    git fetch filtered

    echo ""
    echo "Transient features have been removed in the 'filtered' remote."
    echo "You can inspect it with: git log filtered/main"

    read -p "Apply filtered history to $CURRENT_BRANCH? (yes/no): " APPLY_FILTER

    if [ "$APPLY_FILTER" = "yes" ]; then
        git reset --hard filtered/main
        echo "✓ Filtered history applied"
    fi

    # Cleanup
    git remote remove filtered
    rm -rf "$TEMP_DIR"
else
    echo "git-filter-repo not found. Skipping file removal step."
    echo "Install with: pip install git-filter-repo"
    echo ""
    echo "You can manually remove unwanted files later or continue with commit consolidation."
    read -p "Continue with commit consolidation only? (yes/no): " CONTINUE
    if [ "$CONTINUE" != "yes" ]; then
        echo "Aborted."
        exit 0
    fi
fi

# Step 2: Create new condensed history
echo ""
echo "Step 2: Creating condensed history with 19 commits..."
echo ""

# Create new orphan branch
CONDENSED_BRANCH="main-condensed"
git checkout --orphan "$CONDENSED_BRANCH"

# Remove all files
git rm -rf . 2>/dev/null || true

# Function to create a commit from a point in history
create_commit_from_state() {
    local commit_hash=$1
    local commit_date=$2
    local commit_message=$3
    local commit_body=$4

    echo "Creating: $commit_message"

    # Checkout files from that commit
    git checkout "$BACKUP_BRANCH" "$commit_hash" -- . 2>/dev/null || true

    # Stage all changes
    git add -A

    # Create commit with specified date and message
    GIT_AUTHOR_DATE="$commit_date" GIT_COMMITTER_DATE="$commit_date" \
        git commit -m "$commit_message" -m "$commit_body" --allow-empty
}

# Commit 1
create_commit_from_state "e89e690" "2024-12-06 17:54:56 +0100" \
    "Add RDFSolver class and VOID graph support" \
    "RDFSolver class structure, notebook organization, VOID graph support.

Consolidated commits: e1108aa, e89e690, c1f6537, 85cf378"

# Commit 2
create_commit_from_state "2141bdb" "2025-11-06 18:16:42 +0100" \
    "Replace VOID generator with CONSTRUCT queries" \
    "Change from VOID-generator to CONSTRUCT queries. Add RDF-Config support.

Consolidated 19 commits."

# Commit 3
create_commit_from_state "103b7ed" "2025-11-12 19:08:29 +0100" \
    "Add LinkML schema support and GitHub Actions workflow" \
    "LinkML integration, workflow for notebook generation, Plotly visualizations.

Consolidated 18 commits."

# Commit 4
create_commit_from_state "2d8d39f" "2025-11-17 21:26:11 +0100" \
    "Add query pagination and error handling" \
    "OFFSET/LIMIT pagination, retry logic, query methods, bio2rdf sources.

Consolidated 24 commits."

# Commit 5
create_commit_from_state "bcbc851" "2025-11-20 14:16:12 +0100" \
    "Add JSON-LD support and restructure repository" \
    "JSON-LD support, repository structure changes, PR creation for data files, parser tests.

Consolidated 25 commits."

# Commit 6
create_commit_from_state "119e917" "2025-12-02 14:53:40 +0100" \
    "Separate API and CLI" \
    "API/CLI separation, ReadTheDocs integration, SPARQL→SHACL persistence, tests.

Consolidated 47 commits."

# Commit 7
create_commit_from_state "4b1aa80" "2025-12-11 13:25:52 +0100" \
    "Add Bioregistry namespace analysis and schema export formats" \
    "Bioregistry integration, schema export (LinkML, SHACL, RDF-Config), Makefile, PyPI publishing. Version 0.0.1.

Consolidated 22 commits."

# Commit 8
create_commit_from_state "ea704bc" "2026-01-22 13:17:44 +0100" \
    "Add schema visualization and SPARQL query builder" \
    "Schema cloud visualizations, SPARQL query generator with directionality, IRI resolver.

Consolidated 18 commits."

# Commit 9
create_commit_from_state "5ad893f" "2026-02-20 18:05:54 +0100" \
    "Add SHACL path subsetting and instance matching" \
    "SHACL path subsetting, bioregistry instance matching, SEMRA mappings.

Consolidated 12 commits. Flask backend commits removed from history."

# Commit 10
create_commit_from_state "9b9fc08" "2026-03-03 14:15:37 +0100" \
    "Add QLever local instance support and benchmarking" \
    "QLever local instances, service namespace filtering, benchmarking, decompression tools, data sources, untyped URI handling.

Consolidated 6 commits."

# Commit 11
create_commit_from_state "acfdb18" "2026-03-23 10:57:42 +0100" \
    "Add instance-to-class mapping derivation" \
    "ClassDerivedMapping for class mappings from instance evidence. Version 0.0.2.

Consolidated 22 commits. Flask backend and Docker commits removed."

# Commit 12
create_commit_from_state "e4f285d" "2026-03-27 19:19:47 +0100" \
    "Add Bioregistry and OLS client" \
    "Bioregistry source metadata, OLS v2 REST API client, ontology term resolution, IDSM and PubChem updates.

Consolidated 8 commits."

# Commit 13
create_commit_from_state "6c7ea56" "2026-04-01 11:27:31 +0200" \
    "Add SLURM support for HPC pipeline" \
    "SLURM batch scripts, QLever memory configuration (40G/8G/4G), OOM detection.

Consolidated 15 commits. MCP submodule removed from history."

# Commit 14
create_commit_from_state "283943a" "2026-04-08 15:25:37 +0200" \
    "Fix QLever data ingestion" \
    "IRI sanitization, OBO→TTL conversion, file handling, numeric literal fixes.

Consolidated 7 commits."

# Commit 15
create_commit_from_state "4ae0660" "2026-04-13 15:32:47 +0200" \
    "Refactor pipeline into API and CLI" \
    "Convert shell scripts to Python API and CLI commands.

Consolidated 2 commits."

# Commit 16
create_commit_from_state "1d874e8" "2026-04-14 15:36:27 +0200" \
    "Fix QLever instance lifecycle and error handling" \
    "QLever lifecycle fixes, class mapping pipeline, SLURM mappings-only, STRSTARTS removal.

Consolidated 7 commits."

# Commit 17
create_commit_from_state "f2b652a" "2026-04-20 15:20:33 +0200" \
    "Add edge counting and multi-engine support" \
    "Edge counting, multi-engine support, named graph scoping, LipidMaps source.

Consolidated 4 commits."

# Commit 18
create_commit_from_state "0336366" "2026-04-21 15:09:28 +0200" \
    "Split SLURM pipeline into graph/inference/mapping steps" \
    "Separate SLURM steps for graphs/inference/mappings, verbose logging, OOM handling.

Consolidated 7 commits."

# Commit 19
create_commit_from_state "7bed390" "2026-04-22 15:43:18 +0200" \
    "Add URI discovery and batch query processing" \
    "@context prefix discovery, URI caching, PID tracking, batched queries (50 IRIs max), resolver filtering.

Consolidated 3 commits."

echo ""
echo "✓ Condensed history created on branch: $CONDENSED_BRANCH"
echo ""
echo "=== Next Steps ==="
echo ""
echo "1. Review the new history:"
echo "   git log --oneline"
echo "   git log --stat"
echo ""
echo "2. Compare with original:"
echo "   git log $BACKUP_BRANCH --oneline | wc -l  # Original count"
echo "   git log $CONDENSED_BRANCH --oneline | wc -l  # New count"
echo ""
echo "3. If satisfied, replace main branch:"
echo "   git branch -D main"
echo "   git branch -m $CONDENSED_BRANCH main"
echo ""
echo "4. Force push to remote (WARNING: Destructive!):"
echo "   git push origin main --force"
echo ""
echo "5. Clean up backup (only after confirming everything works):"
echo "   git branch -D $BACKUP_BRANCH"
echo ""
echo "To abort and restore original history:"
echo "   git checkout $BACKUP_BRANCH"
echo "   git branch -D $CONDENSED_BRANCH"
echo ""
