
from dataclasses import dataclass
from typing import Optional

@dataclass(frozen=True)
class Relation:
    catalog: Optional[str]
    schema: str
    name: str

    @property
    def fqn(self) -> str:
        parts = [p for p in [self.catalog, self.schema, self.name] if p]
        return ".".join(parts)

@dataclass
class ModelContext:
    spark: "SparkSession"
    this: Relation
    full_refresh: bool
    materialization: str  # "table" | "view" | "incremental"
    target_exists: bool

    def is_incremental(self) -> bool:
        return (
            self.materialization == "incremental"
            and self.target_exists
            and not self.full_refresh
        )



@model(name="dim_customer", materialization="incremental", unique_key="customer_id")
def dim_customer(ctx: ModelContext):
    src = ctx.spark.table(ctx.ref("stg_customer").fqn)

    if ctx.is_incremental():
        # Example: only process new/changed rows
        return src.where("updated_at >= (select max(updated_at) from " + ctx.this.fqn + ")")
    else:
        return src
