# Troubleshooting Guide: Fixing Diverged Branches

## Problem: Your PR is Blocked - "Only one area can be modified"

If your PR gets blocked, it means you've modified **both folder1 (frontend) AND folder2 (backend)** in the same PR.

### ✅ Quick Fix: Split Your Changes

Split your PR into two separate PRs:

```bash
# Stay on your current branch
git status

# Option 1: If you haven't pushed yet
git reset HEAD~1                    # Undo last commit
git stash                           # Save all changes
git checkout -b fix/backend         # Create backend branch
git stash pop                       # Restore changes
git add folder2/                    # Stage only backend changes
git commit -m "Backend fix"
git push origin fix/backend

# Then create second PR for frontend from original branch
git checkout <your-original-branch>
git checkout -b fix/frontend
git add folder1/                    # Stage only frontend changes
git commit -m "Frontend fix"
git push origin fix/frontend
```

---

## Problem: Your Branch is Out of Sync (Stale)

If the GitHub Actions shows "branch is stale" or "conflicts detected":

### ✅ Solution: Rebase Your Branch

**Option 1: Rebase (Recommended - Cleaner history)**
```bash
# Fetch latest changes from remote
git fetch origin

# Rebase your branch onto the latest base branch
git rebase origin/main

# If there are conflicts, resolve them:
# 1. Open conflicted files and fix them
# 2. Stage the resolved files
git add .

# Continue rebase
git rebase --continue

# Force push your updated branch (safe with --force-with-lease)
git push origin <your-branch-name> --force-with-lease
```

**Option 2: Merge (Keeps full history)**
```bash
git fetch origin
git merge origin/main

# Resolve any conflicts
git add .
git commit -m "Merge main branch"
git push origin <your-branch-name>
```

---

## Problem: Multiple Files Changed Across Folders

### ✅ Solution: Create Separate Branches and PRs

```bash
# If you're on a branch called "fix/all-changes"
# And it has changes in both folder1 and folder2

# 1. Create two new branches from current branch
git branch fix/frontend
git branch fix/backend

# 2. On fix/frontend branch: Keep only folder1 changes
git checkout fix/frontend
git reset origin/main
git add folder1/
git commit -m "Frontend changes"
git push origin fix/frontend

# 3. On fix/backend branch: Keep only folder2 changes
git checkout fix/backend
git reset origin/main
git add folder2/
git commit -m "Backend changes"
git push origin fix/backend

# 4. Close the original PR and create two new ones from these branches
```

---

## Problem: Accidentally Committed to Wrong Branch

### ✅ Solution: Cherry-pick Changes to Correct Branch

```bash
# Get your commit hash from your current branch
git log --oneline -5

# Copy the commit hash (e.g., abc1234)

# Create new branch from main
git checkout origin/main
git checkout -b fix/correct-feature

# Apply only your commit
git cherry-pick abc1234

# Push the new branch
git push origin fix/correct-feature

# On your original branch, undo the wrong commit
git checkout <wrong-branch>
git reset HEAD~1
git push origin <wrong-branch> --force-with-lease
```

---

## Prevention: Best Practices

### 📋 Before You Start
```bash
# 1. Make sure your main branch is up-to-date
git checkout main
git pull origin main

# 2. Create a fresh branch from latest main
git checkout -b fix/my-feature

# 3. Work on ONE area only
```

### 🔍 Before You Commit
```bash
# Check what files you've changed
git status

# Check the diff
git diff

# Verify changes are ONLY in one area (folder1 OR folder2, not both)
```

### 📤 Before You Push
```bash
# Review your commits
git log --oneline -5

# Verify changes one more time
git diff origin/main

# Only push if changes are in ONE area
git push origin fix/my-feature
```

---

## Git Cheat Sheet for This Workflow

| Scenario | Command |
|----------|---------|
| Update your branch | `git fetch origin && git rebase origin/main` |
| Check what changed | `git diff origin/main` |
| See changed files only | `git diff --name-only origin/main` |
| Discard all changes | `git reset --hard origin/main` |
| Undo last commit (keep changes) | `git reset HEAD~1` |
| Undo last commit (lose changes) | `git reset --hard HEAD~1` |
| See git history | `git log --oneline -10` |
| Switch branches | `git checkout <branch-name>` |
| Create new branch | `git checkout -b <new-branch-name>` |

---

## Need More Help?

If you're stuck:
1. Run `git status` to see current state
2. Check `git log --oneline` to see your commits
3. Post the output to the team in a comment
4. We can guide you through the exact steps

**Remember:** It's always safe to pause, ask for help, and not force-push! 🙌

