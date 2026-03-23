## [ERR-20260317-001] shell_cleanup_policy_block

**Logged**: 2026-03-17T02:36:00-04:00
**Priority**: medium
**Status**: pending
**Area**: infra

### Summary
A direct `rm -rf` cleanup command was blocked by policy during Blender source-build setup.

### Error
```
Rejected("`/bin/bash -lc 'rm -rf /tmp/blender-src'` rejected: blocked by policy")
```

### Context
- Command attempted: `rm -rf /tmp/blender-src`
- Purpose: clean up a partial Blender clone before switching to a lighter source fetch path

### Suggested Fix
Prefer non-destructive alternatives or avoid cleanup commands that may trigger policy filters when a fresh path can be used instead.

### Metadata
- Reproducible: yes
- Related Files: none

---
