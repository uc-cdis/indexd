# Aliases for Indexd records

The alias feature in `indexd` allows a client to associate an alias with a document and then retrieve the document by that alias.

There are currently two implementations of an alias system in the codebase, one of which is deprecated. The original alias system (the `/alias` endpoint) was deprecated
because it aliases records by hash instead of by GUID (See https://github.com/uc-cdis/indexd/issues/173). It was replaced by the new alias system (the `/index/{GUID}/aliases` endpoint)
in 11/2019.

## How the current alias system works (`/index/{GUID}/alias` endpoint)

The current alias system allows the client to associate an alias (a text string)
with a document's GUID. (In the indexd codebase, GUIDs are also referred to as `did`s.) An alias cannot be associated with more than one GUID. Once a client has associated an alias with a record, the record can be retrieved by the alias on the root endpoint (`/{alias}`).

**Aliases do not carry over to new versions of a resource**. When a new version of a resource is created with `POST /index/{GUID}`, the new version has a different GUID than
the old version. Aliases are associated with GUIDs, and the old version's aliases do not carry over to the new version's GUID. It is the client's responsibility to migrate aliases
to new versions of a resource if this is the behavior they want.

> NOTE: The current alias system is implemented in `indexd/index/blueprint.py` and uses the `index_record_alias` table. Confusingly, the current alias system is **not** implemented in `/indexd/alias` and does **not** use the `alias_record` table -- these are from the deprecated original alias system.
