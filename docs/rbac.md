
# RBAC in Indexd

## **Problem:**
As an indexd or DRS user, when I list objects, I only expect to see items that belong to projects I have access to.

## **Solution:**

Assuming a Bearer token is included on the request, I expect indexd to query arborist, extract the projects I have access to and add those as an "authz" filter when querying the database.  If a token is not present, I expect that index will still query Arborist to retrieve default permissions.  These calls should be cached for performance.

A feature flag should control this query injection, the flag should default to FALSE, as this will improve chances of getting a PR approved.  All current unit tests should pass.  Additional unit tests should confirm behavior.

## **Alternatives:**
We could have a RBAC aware proxy front end indexd - however will add complexity and processing overhead

## **Context:**

The indexd service is used to manage and serve metadata about data objects, such as files in a data repository. It currently does not enforce any access control on the objects it serves, which means that any user can see all objects regardless of their permissions.

Main [auth code](https://github.com/uc-cdis/indexd/blob/fb21317f2bc72ad9b0ea143fe9388122f59d10f4/indexd/auth/drivers/alchemy.py#L37-L36) has two methods `auth` and `authz`. The [indexd.authorize](https://github.com/uc-cdis/indexd/blob/0859c639f99a7cbce0a0cd15564ed9847814a5ff/indexd/auth/__init__.py#L10) method checks if Basic auth header is present auth is called otherwise authz is called.   The revproxy gateway injects this header [here](https://github.com/uc-cdis/gen3-helm/blob/9ccd25c3e4c40f87f750883802ece5866cdfbc24/helm/revproxy/gen3.nginx.conf/indexd-service.conf#L41-L53) This reliance on Basic auth is concerning and it's rationale is undocumented.  It appears that it is not used for either create or read based on [client API](https://github.com/uc-cdis/indexclient/blob/master/indexclient/client.py)

**Approach:**
Add code to [get_index](https://github.com/uc-cdis/indexd/blob/b6ec68f15a8bb61e99c0daf3f6af729691f213c7/indexd/index/blueprint.py#L60) to call [auth_mapping](https://github.com/uc-cdis/gen3authz/blob/master/src/gen3authz/client/arborist/base.py#L286)
and inject resources (projects) into query.
- [x] skip if feature flag not enabled
- [x] call arborist with token to get resources for user, or without token to get default resources
- [x] Cache arborist results for 30 minutes (typical JWT token lifetime)
- [x] update dependency gen3authz as latest version includes token as parameter (as an alternative to username)
- [x] use [mock_arborist_requests](https://github.com/uc-cdis/indexd/blob/8ff50b9c829920907181d5c186c907e06f5c4a5d/tests/conftest.py#L230) pytest fixture
- [x] ensure all existing tests pass
- [x] add new tests specific to RBAC
- [x] Add feature flag to [default_settings](https://github.com/uc-cdis/indexd/blob/8ff50b9c829920907181d5c186c907e06f5c4a5d/indexd/default_settings.py)
- [x] Remove extraneous logging and debugging code
- [x] Ensure ARE_RECORDS_DISCOVERABLE, GLOBAL_DISCOVERY_AUTHZ See [discussion](https://github.com/uc-cdis/indexd/pull/400#discussion_r2243579240)
- [ ] Add a corresponding feature flag to helm chart

---

## Implementation Overview

* Main changes were made to:
  * indexd/auth
  * indexd/index/drivers/alchemy.py

* All the changes above:
  * should be transparent to the user, and they should not notice any difference in behavior.
  * should be non-breaking, as it only changes the behavior when the `authz` parameter is empty.
  * However, it will throw a 401/403 is the user does not have access to the requested resource,or does not have and Authorization header which is a change from the previous behavior where it would return all the records regardless of the user's access.

* "Breaking" Changes:
  * In order to enforce authorization, we need to ensure that all records have an `authz` field.
  * (This is not a change in behavior to OHSU/ACED/Calypr, but it is a change in behavior to the Indexd API in that effectively authz is mandatory on write)

* Misc:
  * Added stack traces to log for unhandled exceptions see changes to blueprint.py for various endpoints

## **Testing  `tests/rbac`**

The `tests/rbac` suite is designed to validate RBAC-aware behavior in the indexd service, while ensuring the stability and integrity of legacy functionality. The following principles guide its architecture:

- **Preservation of Existing Tests:**  
  All pre-existing tests are retained without modification to guarantee backward compatibility and to ensure that legacy functionality remains unaffected by the introduction of RBAC features.

- **Comprehensive Endpoint Coverage:**  
  New tests are introduced to exercise RBAC logic across a wide set of API endpoints (e.g., list, read, write, update, delete). This ensures that authorization checks are consistently enforced and that the feature flag, token handling, and resource filtering behave as intended in every context.

- **Parameterized Test Design:**  
  Parameterized tests are used to efficiently cover combinations of public, controlled, and private `authz` resources, as well as users with and without tokens. This approach ensures all relevant access patterns are validated, including edge cases, without duplicating test logic.

- **Mocked Authorization Backend:**  
  Arborist responses are mocked to provide deterministic and isolated test scenarios, enabling reliable validation of access control logic without external dependencies.

## **Configuration:**  
  Tests verify both enabled and disabled states of the RBAC feature flag, confirming that the system defaults to legacy behavior unless explicitly configured otherwise.

  * `ARE_RECORDS_DISCOVERABLE`
  
  - **Type:** `bool`
    - **Default:** `True`
    - **Description:**  
      Controls whether any records in IndexD are discoverable via search or listing endpoints.  
      If set to `False`, all records are hidden from discovery, regardless of their individual authorization settings.  
      Note: Role-Based Access Control (RBAC) is not enabled by default.
  
  * `GLOBAL_DISCOVERY_AUTHZ`
  
  - **Type:** `list` or `None`
    - **Default:** `[]`
    - **Description:**  
      Overrides per-record authorization for GET/read operations during record discovery.  
      If set to a list of authorization requirements, these are applied globally to all records for discovery purposes.  
      If set to `None`, the system uses each record's individual `authz` field for authorization checks.  
      This setting does not affect file access permissions, only record discovery.

This approach ensures robust coverage of the new RBAC functionality while maintaining the integrity and reliability of the existing test suite.    