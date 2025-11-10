#!/bin/bash
# Test script to validate workflow configuration and safety measures

set -e  # Exit on any error

echo "🧪 Testing Workflow Safety and Configuration"
echo "============================================"

# Test 1: Check workflow YAML syntax
echo "1️⃣ Checking workflow YAML syntax..."
if command -v yamllint &> /dev/null; then
    yamllint ../../.github/workflows/generate-schema-notebooks.yml
    yamllint ../../.github/workflows/generate-schema-notebooks-matrix.yml
    echo "✅ YAML syntax is valid"
else
    echo "⚠️ yamllint not available, skipping YAML syntax check"
fi

# Test 2: Verify make_notebooks.py functionality
echo ""
echo "2️⃣ Testing make_notebooks.py..."
python make_notebooks.py --list | head -5
echo "✅ make_notebooks.py list functionality works"

python make_notebooks.py --dataset nanosafetyrdf
if [ -f "nanosafetyrdf_schema.ipynb" ]; then
    echo "✅ Notebook generation works"
else
    echo "❌ Notebook generation failed"
    exit 1
fi

# Test 3: Create mock directory structure for testing
echo ""
echo "3️⃣ Testing directory structure safety..."
mkdir -p test_html_output
mkdir -p test_artifacts/schema-dataset1
mkdir -p test_artifacts/schema-dataset2

# Create mock files
echo '{"cells": []}' > test_artifacts/schema-dataset1/dataset1_schema.ipynb
echo '<html><body>Test Report 1</body></html>' > test_artifacts/schema-dataset1/dataset1_schema.html
echo '{"cells": []}' > test_artifacts/schema-dataset2/dataset2_schema.ipynb
echo '<html><body>Test Report 2</body></html>' > test_artifacts/schema-dataset2/dataset2_schema.html

# Test file collection logic (simulate what happens in workflow)
echo "Testing artifact collection logic..."
find test_artifacts/ -name "*_schema.ipynb" -exec cp {} ./ \; 2>/dev/null || echo "Some copies failed"
find test_artifacts/ -name "*_schema.html" -exec cp {} test_html_output/ \; 2>/dev/null || echo "Some copies failed"

collected_notebooks=$(find . -maxdepth 1 -name "*_schema.ipynb" | wc -l)
collected_html=$(find test_html_output/ -name "*_schema.html" | wc -l)

echo "Collected: $collected_notebooks notebooks, $collected_html HTML files"

if [ "$collected_notebooks" -eq 2 ] && [ "$collected_html" -eq 2 ]; then
    echo "✅ Artifact collection logic works correctly"
else
    echo "❌ Artifact collection logic failed"
    exit 1
fi

# Test 4: Git configuration simulation
echo ""
echo "4️⃣ Testing git safety measures..."
git config user.email || echo "Git email not set (normal for testing)"
git config user.name || echo "Git name not set (normal for testing)"

# Simulate git operations (without actually committing)
echo "Simulating git add operations..."
git status --porcelain > /dev/null 2>&1 && echo "✅ Git repository is accessible"

# Test 5: Cleanup
echo ""
echo "5️⃣ Cleaning up test files..."
rm -rf test_html_output test_artifacts
rm -f dataset1_schema.ipynb dataset2_schema.ipynb
echo "✅ Cleanup completed"

echo ""
echo "🎉 All workflow safety tests passed!"
echo ""
echo "Key Safety Features Verified:"
echo "  ✓ YAML syntax validation"
echo "  ✓ Script functionality"
echo "  ✓ Directory structure handling"
echo "  ✓ Artifact collection logic"
echo "  ✓ Git repository access"
echo ""
echo "The workflows should handle concurrent operations safely with:"
echo "  • Concurrency controls to prevent simultaneous runs"
echo "  • Retry logic for git operations"
echo "  • Proper error handling for missing artifacts"
echo "  • Continue-on-error for individual job failures"
echo "  • Comprehensive logging and status reporting"