# Testing GitHub Actions Locally with act

This guide explains how to test Koheesio's GitHub Actions workflows locally using `act`, enabling faster development cycles and reducing CI resource usage.

## Table of Contents

- [What is act?](#what-is-act)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Basic Usage](#basic-usage)
- [Testing Strategies](#testing-strategies)
- [Troubleshooting](#troubleshooting)
- [Best Practices](#best-practices)
- [Advanced Usage](#advanced-usage)

## What is act?

[act](https://github.com/nektos/act) is a tool that runs GitHub Actions locally using Docker-compatible container runtimes. It reads your `.github/workflows/*.yml` files and simulates the GitHub Actions environment on your machine.

**Benefits:**

- Test workflows before pushing to GitHub
- Faster feedback loops (no waiting for CI)
- Reduced CI minutes usage
- Debug workflows with full container access
- Work offline (after initial image download)

## Prerequisites

### 1. Container Runtime (Required)

`act` requires a Docker-compatible container runtime. You have several options:

#### Option A: Colima (Recommended for macOS/Linux - Free & Open Source)

Lightweight, free, and no licensing issues.

\`\`\`bash
# Install
brew install colima

# Install Docker CLI (Colima requires it)
brew install docker

# Start Colima
colima start --cpu 4 --memory 8
\`\`\`

**Pros:** Free, lightweight, no enterprise licensing concerns  
**Cons:** macOS/Linux only

#### Option B: Docker Desktop

If your organization has Docker Desktop licenses.

[Download Docker Desktop](https://www.docker.com/products/docker-desktop)

**Pros:** Official, well-supported, GUI included  
**Cons:** Requires commercial license for large organizations

#### Option C: OrbStack (macOS Alternative)

Modern, fast alternative for macOS.

\`\`\`bash
brew install orbstack
\`\`\`

**Pros:** Fast startup, modern UI, free tier available  
**Cons:** macOS only, some features require paid version

#### Option D: Podman (Fully Open Source)

Completely free alternative, Docker-compatible.

\`\`\`bash
brew install podman
podman machine init
podman machine start
\`\`\`

**Pros:** Completely free, no licensing issues  
**Cons:** May require additional configuration for some use cases

### 2. Make

Should be pre-installed on macOS/Linux. For Windows, install via WSL or use Git Bash.

### Verification

After installing your container runtime:

\`\`\`bash
make act-check
\`\`\`

This verifies both `act` and your container runtime are properly configured.

## Installation

### Install act

#### Using Make (Recommended)

\`\`\`bash
make act-install
\`\`\`

This automatically detects your platform and installs `act` using the appropriate method.

#### Manual Installation

**macOS:**
\`\`\`bash
brew install act
\`\`\`

**Linux:**
\`\`\`bash
curl https://raw.githubusercontent.com/nektos/act/master/install.sh | sudo bash
\`\`\`

**Windows:**
\`\`\`powershell
# Using winget
winget install nektos.act

# Using Chocolatey
choco install act-cli

# Using Scoop
scoop install act
\`\`\`

### Verify Installation

\`\`\`bash
make act-check
\`\`\`

This command checks:
- ✅ act is installed
- ✅ Docker-compatible runtime is available
- ✅ Container runtime is running

## Basic Usage

### List Available Workflows

\`\`\`bash
make ci-list
\`\`\`

This shows all workflows and jobs defined in `.github/workflows/`.

### Run a Specific Test Matrix Combination

\`\`\`bash
make ci-test-matrix PYTHON=3.12 PYSPARK=352r
\`\`\`

**Available combinations:**
- Python versions: `3.10`, `3.11`, `3.12`
- PySpark versions: `33`, `34`, `35`, `35r`, `352`, `352r`

### Dry Run (See What Would Execute)

\`\`\`bash
make ci-dry-run
\`\`\`

Shows what would run without actually executing - useful for validating workflow changes.

### Run Full Test Workflow

\`\`\`bash
make ci-test
\`\`\`

⚠️ **Warning:** This runs ALL test matrix combinations and may take 30+ minutes.

## Testing Strategies

### Strategy 1: Test Your Changes Quickly

When you've made changes, test one relevant combination:

\`\`\`bash
# For Python 3.12 changes
make ci-test-matrix PYTHON=3.12 PYSPARK=352r

# For PySpark 3.4 compatibility
make ci-test-matrix PYTHON=3.10 PYSPARK=34
\`\`\`

### Strategy 2: Test Multiple Combinations

Create a simple script to test multiple combinations:

\`\`\`bash
#!/bin/bash
for python in 3.10 3.11 3.12; do
  echo "Testing Python \$python..."
  make ci-test-matrix PYTHON=\$python PYSPARK=35
done
\`\`\`

### Strategy 3: Validate Workflow Changes

When modifying `.github/workflows/*.yml`:

\`\`\`bash
# Check syntax and what would run
make ci-dry-run

# Run a quick combination to verify
make ci-test-matrix PYTHON=3.12 PYSPARK=352r
\`\`\`

### Strategy 4: Test Before Pushing

Catch issues early:

\`\`\`bash
# Test the most common configuration
make ci-test-matrix PYTHON=3.12 PYSPARK=352r

# If that passes, push with confidence
git push
\`\`\`

## Troubleshooting

### Container Runtime Not Running

**Error:** `❌ Container runtime is not running`

**Solution:**

\`\`\`bash
# Colima
colima start

# Docker Desktop
# Start the Docker Desktop application

# Podman
podman machine start

# OrbStack
# Start the OrbStack application
\`\`\`

### act Not Found

**Error:** `❌ act is not installed`

**Solution:**
\`\`\`bash
make act-install
# Or install manually (see Installation section above)
\`\`\`

### Architecture Mismatch (M1/M2 Macs)

**Error:** Build failures or warnings about architecture

**Solution:** The `.actrc` file already configures `--container-architecture linux/amd64`. This should work automatically.

If issues persist:

\`\`\`bash
act -j tests --container-architecture linux/amd64 --matrix python-version:3.12 --matrix pyspark-version:352r
\`\`\`

### Slow First Run

**Expected:** First run downloads Docker images (~1-2GB). Subsequent runs are much faster.

**Solution:** Be patient, or pre-download:
\`\`\`bash
docker pull catthehacker/ubuntu:act-latest
\`\`\`

### Out of Disk Space

**Solution:** Clean up Docker:
\`\`\`bash
docker system prune -a
\`\`\`

### Permission Denied (Linux)

**Error:** `permission denied while trying to connect to the Docker daemon socket`

**Solution:**
\`\`\`bash
# Add your user to the docker group
sudo usermod -aG docker \$USER

# Log out and back in, or run:
newgrp docker
\`\`\`

### Tests Pass Locally But Fail in CI

**Possible causes:**

1. **Different Hatch versions** - Run `make hatch-upgrade`
2. **Cached dependencies** - Delete `.venvs/` directory and recreate
3. **Environment differences** - Check GitHub Actions logs for specifics
4. **Secrets/Environment variables** - CI may have variables not set locally

## Best Practices

### 1. Test Before Pushing

Run at least one relevant combination before pushing:

\`\`\`bash
make ci-test-matrix PYTHON=3.12 PYSPARK=352r
\`\`\`

### 2. Use Dry Run for Workflow Changes

When modifying workflow files:

\`\`\`bash
make ci-dry-run
\`\`\`

### 3. Keep Container Images Updated

Periodically update:

\`\`\`bash
act --pull  # Force pull latest images
\`\`\`

### 4. Test Edge Cases

Test the minimum and maximum supported versions:

\`\`\`bash
# Minimum
make ci-test-matrix PYTHON=3.10 PYSPARK=33

# Maximum  
make ci-test-matrix PYTHON=3.12 PYSPARK=352r
\`\`\`

### 5. Clean Up After Testing

\`\`\`bash
# Remove act-created containers
docker ps -a | grep act- | awk '{print \$1}' | xargs docker rm

# Clean up volumes
docker volume prune

# Full cleanup (reclaim disk space)
docker system prune -a
\`\`\`

### 6. Use Specific Test Patterns

When you know which tests are affected:

\`\`\`bash
# Run specific test directory
make ci-test-matrix PYTHON=3.12 PYSPARK=352r TEST=tests/asyncio/

# Run specific test pattern
act pull_request -j tests --matrix python-version:3.12 --matrix pyspark-version:352r --env PYTEST_ARGS="-k test_async"
\`\`\`

## Advanced Usage

### Running Specific Jobs

\`\`\`bash
act pull_request -j tests
\`\`\`

### Setting Environment Variables

\`\`\`bash
act pull_request -j tests --env PYTEST_ARGS="-k test_specific"
\`\`\`

### Using Secrets

Create `.secrets` file (add to `.gitignore`):

\`\`\`
GITHUB_TOKEN=ghp_xxx
\`\`\`

Then run:

\`\`\`bash
act --secret-file .secrets
\`\`\`

### Debugging

Enable verbose output:

\`\`\`bash
act -v pull_request -j tests --matrix python-version:3.12 --matrix pyspark-version:352r
\`\`\`

### Custom Runner Images

If you need specific runner images, create or modify `.actrc`:

\`\`\`
-P ubuntu-latest=catthehacker/ubuntu:act-latest
\`\`\`

### Running Other Workflows

\`\`\`bash
# List all available workflows
act -l

# Run a specific workflow
act workflow_dispatch -W .github/workflows/release.yml
\`\`\`

## Configuration Files

### `.actrc`

Located in the repository root, contains default `act` configuration:

- Architecture settings (`--container-architecture linux/amd64`)
- Image pull behavior (`--pull=false`)
- .gitignore usage (`--use-gitignore=true`)

You can override these per-command if needed.

### `.github/act/README.md`

Quick reference for common commands.

## Performance Tips

### 1. Use Specific Matrix Combinations

Instead of running the full matrix, test only what you need:

\`\`\`bash
make ci-test-matrix PYTHON=3.12 PYSPARK=352r
\`\`\`

### 2. Reuse Containers

The `.actrc` configuration already sets `--pull=false` to reuse images.

### 3. Increase Container Resources

If using Colima:

\`\`\`bash
colima stop
colima start --cpu 8 --memory 16
\`\`\`

### 4. Parallel Testing

The workflow already uses parallel testing (`xdist`), but you can adjust workers:

\`\`\`bash
act -j tests --env PYTEST_XDIST_AUTO_NUM_WORKERS=4
\`\`\`

## Container Runtime Comparison

| Runtime | License | Platform | Installation | Speed | Disk Usage |
|---------|---------|----------|--------------|-------|------------|
| **Colima** | Free (MIT) | macOS, Linux | \`brew install colima docker\` | Fast | Low |
| **Docker Desktop** | Commercial* | macOS, Windows, Linux | Download GUI installer | Medium | Medium |
| **OrbStack** | Freemium | macOS | \`brew install orbstack\` | Very Fast | Low |
| **Podman** | Free (Apache) | All platforms | \`brew install podman\` | Fast | Low |

\* Free for personal use, small businesses. Requires license for larger organizations.

**Recommendation:** Use **Colima** if you're on macOS/Linux and want a free, lightweight solution.

## Resources

- [act Documentation](https://github.com/nektos/act)
- [act Runner Images](https://github.com/catthehacker/docker_images)
- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [Colima Documentation](https://github.com/abiosoft/colima)
- [Podman Documentation](https://podman.io/)

## Support

If you encounter issues:

1. Check this documentation
2. Run `make act-check` to verify installation
3. Check your container runtime is running and has sufficient resources
4. Check the [act GitHub repository issues](https://github.com/nektos/act/issues)
5. Check your container runtime's documentation

## FAQ

**Q: Why do I need a container runtime?**  
A: `act` simulates GitHub Actions by running workflows in containers, just like GitHub does.

**Q: Can I use this on Windows?**  
A: Yes, but you'll need WSL2 or use act with Docker Desktop on Windows.

**Q: Will this use my GitHub Actions minutes?**  
A: No! Running locally with `act` uses zero GitHub Actions minutes.

**Q: Can I test private actions?**  
A: Yes, but you may need to configure secrets/tokens for private repository access.

**Q: How much disk space do I need?**  
A: Initial setup: ~2-3GB for images. Recommend having at least 10GB free.

**Q: Can I run multiple combinations in parallel?**  
A: Yes, but be careful with resource usage. Run multiple `make ci-test-matrix` commands in different terminals.

## Workflow Compatibility

The test workflow (`.github/workflows/test.yml`) has been configured to work seamlessly with both GitHub Actions and `act`:

- **In GitHub Actions**: Uses the official `pypa/hatch@install` action for fast, pre-compiled Hatch binaries
- **In act (local)**: Automatically detects the `ACT` environment variable and installs Hatch via pip instead

This ensures the workflow runs identically whether in CI or locally, with no manual modifications needed.

### How It Works

```yaml
- name: Install Hatch (for local act)
  if: ${{ env.ACT }}
  run: pip install hatch

- name: Install Hatch (for GitHub Actions)
  if: ${{ !env.ACT }}
  uses: pypa/hatch@install
```

The `ACT` environment variable is automatically set by act, enabling this conditional logic.
