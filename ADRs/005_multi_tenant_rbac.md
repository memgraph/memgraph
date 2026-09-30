# Multiple Roles Per User Support

## Author
Andreja Tonev (https://github.com/andrejtonev)

## Status
**ACCEPTED**

## Date
2025-07-15

## Problem
Memgraph's authentication system previously supported only single role per user, creating two critical limitations:

1. **SSO Integration**: External identity providers (OIDC, SAML) return arrays of roles, but Memgraph could only handle single roles, forcing organizations to either lose role information or implement complex workarounds.

2. **Multi-tenant RBAC**: Organizations need users to have different roles for different databases (tenants), but the single-role model prevented database-specific role assignments, limiting proper tenant isolation.

## Criteria
1. **SSO Compatibility**: Must support arrays of roles from external identity providers
2. **Multi-tenant Support**: Must enable database-specific role assignments
3. **Backward Compatibility**: Existing single-role users must continue to work without changes

## Decision
Implement multiple roles per user support to address both SSO requirements and multi-tenant RBAC needs. This unified approach solves both problems while maintaining backward compatibility.

**Architecture Changes:**
- Enhanced User Model supporting multiple roles with database-specific mappings
- New Roles Container class managing role collections with database-aware filtering
- Three-key storage approach (user data, global roles, multi-tenant mappings)
- Database-aware permission resolution combining all applicable roles

**SSO Integration:**
- Auth modules now accept both single role strings and role arrays
- Backward-compatible parsing: single role strings continue to work
- Role mapping from external providers to Memgraph roles

**Multi-tenant RBAC:**
- Database-specific role assignments using dedicated methods
- Bidirectional mapping between databases and roles inside the User class
- Permission filtering based on database context

**API Changes:**
- New Cypher syntax for database-specific role assignment
- Enhanced auth module responses supporting multiple roles
- Backward-compatible commands for existing single-role users

**Consequences:**
- **Positive**: Enables SSO integration, multi-tenant isolation, flexible permissions
- **Negative**: Increased complexity, storage overhead
- **Risks**: Permission conflicts resolved by deny-takes-precedence, graceful error handling
