---
title: kelp.meta
---

# kelp.meta

Reference for the MetaFramework — the reusable metadata loading backend that powers Kelp's runtime context. Frameworks subclass `MetaFramework`, declare a `MetaProjectSpec`, and call `init()` / `get_context()` to manage lifecycle and catalog access.

::: kelp.meta.framework.MetaFramework

::: kelp.meta.context.MetaRuntimeContext

::: kelp.meta.catalog_index.MetaCatalog

::: kelp.meta.spec.MetaProjectSpec

::: kelp.meta.spec.MetaObjectSpec
