"""Integration tests for ODCS import/export.

Tests the real custom importer and exporter implementations without mocking
the conversion logic. Only DQX LLM module and WorkspaceClient are mocked
for DQX rule generation (due to known bug).
"""

from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from datacontract.data_contract import DataContract
from datacontract.export.exporter_factory import exporter_factory
from datacontract.imports.importer_factory import importer_factory

from kelp.cli.odcs.app import export_contract, import_contract
from kelp.integration.odcs.custom_exporter import (
    KelpExporter,
    _add_dqx_rules_to_models,
    datacontract_to_kelp_models,
)
from kelp.integration.odcs.custom_importer import (
    KelpImporter,
    export_to_contract,
    kelp_model_to_datacontract,
)
from kelp.models.model import (
    Column,
    DQXQuality,
    ForeignKeyConstraint,
    Model,
    PrimaryKeyConstraint,
    SDPQuality,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"

# Register kelp exporter/importer with datacontract factories
exporter_factory.register_exporter("kelp", KelpExporter)  # ty: ignore[invalid-argument-type]
importer_factory.register_importer("kelp", KelpImporter)  # ty: ignore[invalid-argument-type]


@pytest.fixture
def orders_contract():
    """Load the orders test contract fixture."""
    from datacontract.data_contract import resolve

    return resolve.resolve_data_contract(
        data_contract_location=str(FIXTURES_DIR / "orders_contract.yml")
    )


@pytest.fixture
def sample_kelp_model() -> Model:
    """Create a sample kelp Model for export testing."""
    return Model(
        name="customers",
        description="Customer dimension table",
        quality=DQXQuality(
            engine="dqx",
            checks=[
                {
                    "name": "email_not_empty",
                    "criticality": "error",
                    "check": {
                        "function": "is_not_null_and_not_empty",
                        "arguments": {"column": "email"},
                    },
                }
            ],
        ),
        constraints=[
            PrimaryKeyConstraint(name="pk_customers", columns=["customer_id"]),
            ForeignKeyConstraint(
                name="fk_customers_country",
                columns=["country_code"],
                reference_table="countries",
                reference_columns=["code"],
            ),
        ],
        columns=[
            Column(name="customer_id", data_type="STRING", nullable=False, description="Unique ID"),
            Column(name="email", data_type="STRING", nullable=False, description="Email address"),
            Column(name="name", data_type="STRING", nullable=True, description="Full name"),
            Column(name="country_code", data_type="STRING", nullable=False, description="Country"),
            Column(name="created_at", data_type="TIMESTAMP", nullable=False, description="Created"),
        ],
        partition_cols=["created_at"],
        tags={"finance": "core", "region": ""},
        meta={
            "odcs_domain": "crm",
            "odcs_status": "active",
            "odcs_owner": "data-platform",
            "odcs_id": "urn:datacontract:crm:customers",
            "odcs_version": "2.1.0",
            "lineage": "hubspot_sync",
            "retention": "90d",
        },
    )


class TestImportFromContract:
    """Test importing a data contract into kelp Models."""

    def test_import_produces_correct_model(self, orders_contract) -> None:
        """Test that importing a contract produces a correct kelp Model."""
        models = datacontract_to_kelp_models(orders_contract)

        assert len(models) == 1
        model = models[0]
        assert model.name == "orders"
        assert model.description == "Customer orders table"

    def test_import_columns(self, orders_contract) -> None:
        """Test that columns are correctly mapped."""
        models = datacontract_to_kelp_models(orders_contract)
        model = models[0]

        assert len(model.columns) == 5

        # Check order_id
        order_id = model.columns[0]
        assert order_id.name == "order_id"
        assert order_id.data_type == "string"
        assert order_id.nullable is False
        assert order_id.description == "Unique order identifier"

        # Check amount
        amount = model.columns[2]
        assert amount.name == "amount"
        assert amount.data_type == "decimal(10,2)"
        assert amount.nullable is False

        # Check status (nullable)
        status = model.columns[3]
        assert status.name == "status"
        assert status.nullable is True

    def test_import_quality_checks(self, orders_contract) -> None:
        """Test that DQX quality checks are extracted."""
        models = datacontract_to_kelp_models(orders_contract)
        model = models[0]

        assert model.quality is not None
        assert isinstance(model.quality, DQXQuality)
        assert model.quality.engine == "dqx"
        # Should have property-level (amount) + schema-level (minimum_orders) checks
        assert len(model.quality.checks) == 2
        check_names = [c["name"] for c in model.quality.checks]
        assert "amount_positive" in check_names
        assert "minimum_orders" in check_names

    def test_import_contract_metadata_into_meta(self, orders_contract) -> None:
        """Test that contract domain/status become model meta entries."""
        models = datacontract_to_kelp_models(orders_contract)
        model = models[0]

        assert model.meta.get("odcs_domain") == "checkout"
        assert model.meta.get("odcs_status") == "active"
        assert "odcs_domain" not in model.tags
        assert "odcs_status" not in model.tags

    def test_import_constraints_include_explicit_type(self) -> None:
        """Test that imported constraints contain explicit constraint type values."""
        yaml_contract = """
kind: DataContract
apiVersion: v3.0.2
id: urn:datacontract:test:constraints
name: Constraints Contract
version: 1.0.0
domain: test
status: active
schema:
  - name: child
    physicalType: table
    properties:
      - name: id
        physicalType: string
        required: true
        primaryKey: true
        primaryKeyPosition: 1
      - name: parent_id
        physicalType: string
        required: false
        relationships:
          - type: foreignKey
            to: parent.id
    relationships:
      - type: foreignKey
        from: child.parent_id
        to: parent.id
"""
        dc = DataContract(data_contract_str=yaml_contract)
        models = datacontract_to_kelp_models(dc.get_data_contract())
        model = models[0]

        assert len(model.constraints) == 3
        constraint_types = [constraint.type for constraint in model.constraints]
        assert "primary_key" in constraint_types
        assert constraint_types.count("foreign_key") == 2


class TestExportToContract:
    """Test exporting kelp Models to data contract format."""

    def test_export_produces_valid_contract(self, sample_kelp_model: Model) -> None:
        """Test that export produces a valid OpenDataContractStandard."""
        contract = export_to_contract(sample_kelp_model)

        assert contract.kind == "DataContract"
        assert contract.apiVersion == "v3.1.0"
        assert contract.name == "customers"
        assert contract.domain == "crm"
        assert contract.status == "active"
        assert contract.id == "urn:datacontract:crm:customers"
        assert contract.version == "2.1.0"
        assert contract.customProperties is not None
        custom = {p.property: p.value for p in contract.customProperties}
        assert custom["owner"] == "data-platform"
        assert custom["lineage"] == "hubspot_sync"
        assert custom["retention"] == "90d"

    def test_export_schema_properties(self, sample_kelp_model: Model) -> None:
        """Test that columns are mapped to schema properties."""
        contract = export_to_contract(sample_kelp_model)

        assert contract.schema_ is not None
        assert len(contract.schema_) == 1
        schema = contract.schema_[0]
        assert schema.name == "customers"
        assert schema.physicalType == "table"
        assert schema.tags is not None
        assert "finance:core" in schema.tags
        assert "region" in schema.tags
        assert schema.properties is not None
        assert len(schema.properties) == 5

        # Check customer_id
        cid = schema.properties[0]
        assert cid.name == "customer_id"
        assert cid.physicalType == "STRING"
        assert cid.required is True
        assert cid.description == "Unique ID"

        # Check name (nullable)
        name_prop = schema.properties[2]
        assert name_prop.name == "name"
        assert name_prop.required is False

        created = next(p for p in schema.properties if p.name == "created_at")
        assert created.partitioned is True
        assert created.partitionKeyPosition == 1

    def test_export_quality_checks(self, sample_kelp_model: Model) -> None:
        """Test that DQX quality checks are mapped to contract quality."""
        contract = export_to_contract(sample_kelp_model)

        assert contract.schema_ is not None
        schema = contract.schema_[0]
        assert schema.quality is not None
        assert len(schema.quality) == 1
        dq = schema.quality[0]
        assert dq.type == "custom"
        assert dq.engine == "dqx"
        assert isinstance(dq.implementation, dict)
        assert dq.implementation["name"] == "email_not_empty"

    def test_export_constraints_to_odcs_relationships(self, sample_kelp_model: Model) -> None:
        """Test that PK/FK constraints map to ODCS property metadata and relationships."""
        contract = export_to_contract(sample_kelp_model)
        assert contract.schema_ is not None
        schema = contract.schema_[0]
        assert schema.properties is not None

        customer_id = next(p for p in schema.properties if p.name == "customer_id")
        assert customer_id.primaryKey is True
        assert customer_id.primaryKeyPosition == 1

        country_code = next(p for p in schema.properties if p.name == "country_code")
        assert country_code.relationships is not None
        assert country_code.relationships[0].type == "foreignKey"
        assert country_code.relationships[0].to == "countries.code"

        assert schema.customProperties is not None
        schema_custom = {p.property: p.value for p in schema.customProperties}
        assert schema_custom["lineage"] == "hubspot_sync"

    def test_export_sdp_quality_passthrough_dict(self) -> None:
        """SDP quality should be passed as original dict in implementation."""
        sdp_model = Model(
            name="bronze_customers",
            columns=[
                Column(name="_rescued_data", data_type="string", nullable=True),
            ],
            quality=SDPQuality(
                engine="sdp",
                expect_all_or_fail={"_rescued_data_is_null": "_rescued_data IS NULL"},
            ),
        )

        contract = kelp_model_to_datacontract(sdp_model)
        assert contract.schema_ is not None
        schema = contract.schema_[0]

        assert schema.quality is not None
        assert len(schema.quality) == 1
        dq = schema.quality[0]
        assert dq.engine == "sdp"
        assert isinstance(dq.implementation, dict)
        assert dq.implementation.get("engine") is None
        assert dq.implementation.get("expect_all_or_fail", {}).get("_rescued_data_is_null") == (
            "_rescued_data IS NULL"
        )

        models = datacontract_to_kelp_models(contract)
        assert models[0].quality is not None
        assert models[0].quality.engine == "sdp"
        assert isinstance(models[0].quality, SDPQuality)
        assert models[0].quality.expect_all_or_fail.get("_rescued_data_is_null") == (
            "_rescued_data IS NULL"
        )

    def test_export_to_yaml_string(self, sample_kelp_model: Model) -> None:
        """Test that exported contract can be serialized to YAML."""
        contract = export_to_contract(sample_kelp_model)
        yaml_str = contract.to_yaml()

        assert "DataContract" in yaml_str
        assert "customers" in yaml_str
        assert "customer_id" in yaml_str

    def test_import_custom_properties_and_tag_values_to_meta_and_tags(self) -> None:
        """Contract/schema customProperties and key:value tags should map to meta/tags."""
        yaml_contract = """
kind: DataContract
apiVersion: v3.1.0
id: urn:datacontract:test:tagmeta
name: Tag Meta Contract
version: 1.0.0
domain: analytics
status: active
customProperties:
  - property: retention
    value: 30d
  - property: owner
    value: team-analytics
schema:
  - name: customers
    physicalType: table
    tags:
      - tier:gold
      - pii
    customProperties:
      - property: lineage
        value: cdc
    properties:
      - name: country
        physicalType: string
        tags:
          - class:internal
"""
        dc = DataContract(data_contract_str=yaml_contract)
        model = datacontract_to_kelp_models(dc.get_data_contract())[0]

        assert model.tags.get("tier") == "gold"
        assert model.tags.get("pii") == ""
        assert model.columns[0].tags.get("class") == "internal"
        assert model.meta.get("retention") == "30d"
        assert model.meta.get("lineage") == "cdc"
        assert model.meta.get("odcs_domain") == "analytics"
        assert model.meta.get("odcs_status") == "active"
        assert model.meta.get("odcs_owner") == "team-analytics"

    def test_dqx_rules_append_deduplicates_by_name(self) -> None:
        """DQX rules should not be duplicated when check names already exist."""
        models = [
            {
                "name": "orders",
                "quality": {
                    "engine": "dqx",
                    "checks": [
                        {"name": "amount_positive", "check": {"function": "sql_expression"}},
                    ],
                },
            }
        ]
        new_rules = [
            {"name": "amount_positive", "user_metadata": {"schema": "orders"}},
            {"name": "freshness", "user_metadata": {"schema": "orders"}},
        ]

        result = _add_dqx_rules_to_models(models, new_rules)
        checks = result[0]["quality"]["checks"]
        check_names = [check["name"] for check in checks]
        assert check_names.count("amount_positive") == 1
        assert "freshness" in check_names

    def test_dqx_rules_append_when_quality_missing(self) -> None:
        """DQX rules should be appended when model has no quality field yet."""
        models = [{"name": "orders"}]
        new_rules = [
            {"name": "freshness", "user_metadata": {"schema": "orders"}},
        ]

        result = _add_dqx_rules_to_models(models, new_rules)
        assert result[0]["quality"]["engine"] == "dqx"
        assert [check["name"] for check in result[0]["quality"]["checks"]] == ["freshness"]

    def test_dqx_rules_skip_when_quality_engine_is_not_dqx(self) -> None:
        """DQX rules should be skipped only when a non-dqx engine is explicitly set."""
        models = [
            {
                "name": "orders",
                "quality": {"engine": "sdp", "expect_all_or_fail": {"ok": "1=1"}},
            }
        ]
        new_rules = [
            {"name": "freshness", "user_metadata": {"schema": "orders"}},
        ]

        result = _add_dqx_rules_to_models(models, new_rules)
        assert result[0]["quality"]["engine"] == "sdp"
        assert "checks" not in result[0]["quality"]


class TestRoundTrip:
    """Test import -> export and export -> import roundtrips."""

    def test_import_then_export_preserves_structure(self, orders_contract) -> None:
        """Test that importing then exporting preserves core structure."""
        # Import contract to kelp
        models = datacontract_to_kelp_models(orders_contract)
        assert len(models) == 1

        # Export back to contract
        exported = kelp_model_to_datacontract(models[0])

        assert exported.name == "orders"
        assert exported.schema_ is not None
        assert len(exported.schema_) == 1
        assert exported.schema_[0].name == "orders"
        assert exported.schema_[0].properties is not None
        assert len(exported.schema_[0].properties) == 5

    def test_export_then_import_preserves_structure(self, sample_kelp_model: Model) -> None:
        """Test that exporting then importing preserves core structure."""
        # Export to contract
        contract = kelp_model_to_datacontract(sample_kelp_model)

        # Import back to kelp
        models = datacontract_to_kelp_models(contract)

        assert len(models) == 1
        model = models[0]
        assert model.name == "customers"
        assert len(model.columns) == 5
        assert model.columns[0].name == "customer_id"
        assert model.columns[0].nullable is False


class TestKelpExporterClass:
    """Test the KelpExporter class via DataContract.export()."""

    def test_export_via_datacontract(self) -> None:
        """Test that exporting via DataContract produces a kelp Model."""
        dc = DataContract(data_contract_file=str(FIXTURES_DIR / "orders_contract.yml"))
        result = dc.export(export_format="kelp")  # ty: ignore[invalid-argument-type]
        model = result[0] if isinstance(result, list) else result

        assert isinstance(model, Model)
        assert model.name == "orders"


class TestKelpImporterClass:
    """Test the KelpImporter class with model-first inputs."""

    def test_importer_accepts_model_input(self, sample_kelp_model: Model) -> None:
        """Importer should accept Model directly as primary input."""
        importer = KelpImporter(import_format="kelp")
        result = importer.import_source(
            source=sample_kelp_model,
            import_args={"name": "customers_contract"},
        )

        assert result.name == "customers_contract"
        assert result.schema_ is not None
        assert len(result.schema_) == 1
        assert result.schema_[0].name == "customers"

    def test_importer_rejects_non_model_input(self) -> None:
        """Importer should reject non-Model input types."""
        importer = KelpImporter(import_format="kelp")

        with pytest.raises(ValueError, match="only accepts a single Model"):
            importer.import_source(source="not-a-model", import_args={})  # ty: ignore[invalid-argument-type]


class TestDQXRuleGeneration:
    """Test DQX rule generation from data contracts."""

    def test_generate_dqx_rules_via_exporter(self) -> None:
        """Test DQX rule generation via KelpExporter with generate_dqx_rules=True."""
        dc = DataContract(data_contract_file=str(FIXTURES_DIR / "orders_contract.yml"))
        result = dc.export(export_format="kelp", generate_dqx_rules=True)  # ty: ignore[invalid-argument-type]
        model = result[0] if isinstance(result, list) else result

        assert isinstance(model, Model)
        assert isinstance(model.quality, DQXQuality)
        assert len(model.quality.checks) > 0


class TestExportCli:
    """Test ODCS export CLI command behavior."""

    def test_export_cli_uses_importer_and_writes_output(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        sample_kelp_model: Model,
    ) -> None:
        """Export command should convert catalog model to ODCS YAML output."""

        def _fake_init(**kwargs):
            return SimpleNamespace(catalog={"models": [sample_kelp_model]})

        monkeypatch.setattr("kelp.config.init", _fake_init)

        output_file = tmp_path / "contract.yml"
        export_contract(model="customers", output=output_file)

        output = output_file.read_text(encoding="utf-8")
        assert "kind: DataContract" in output
        assert "name: customers" in output
        assert "domain: crm" in output

    def test_export_cli_include_server_uses_model_catalog_schema(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        sample_kelp_model: Model,
    ) -> None:
        """include-server should add ODCS server fields from model catalog/schema."""
        with_server = sample_kelp_model.model_copy(deep=True)
        with_server.catalog = "main"
        with_server.schema_ = "bronze"

        def _fake_init(**kwargs):
            return SimpleNamespace(catalog={"models": [with_server]})

        monkeypatch.setattr("kelp.config.init", _fake_init)

        output_file = tmp_path / "contract_server.yml"
        export_contract(model="customers", output=output_file, include_server=True)

        output = output_file.read_text(encoding="utf-8")
        assert "servers:" in output
        assert "type: databricks" in output
        assert "catalog: main" in output
        assert "schema: bronze" in output


class TestPatchCli:
    """Test ODCS CLI patch modes."""

    def test_import_cli_patch_uses_yaml_manager(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
        tmp_path: Path,
    ) -> None:
        """Import patch mode should call YamlManager patch and preserve patch workflow."""
        from kelp.service.yaml_manager import YamlUpdateReport

        called_models: list[Model] = []

        class _CatalogIndexStub:
            def get_index(self, name: str):
                if name == "models":
                    return {}
                return {}

        class _ContextStub:
            def __init__(self) -> None:
                self.catalog_index = _CatalogIndexStub()

        def _fake_init(**kwargs):
            return None

        def _fake_get_context():
            return _ContextStub()

        def _fake_patch_model_yaml(model: Model, **kwargs):
            called_models.append(model)
            return YamlUpdateReport(
                model_name=model.name,
                file_path=tmp_path / "fake.yml",
                result_model={"name": model.name},
                changes_made=True,
                added_fields=["name"],
                updated_fields=[],
                removed_fields=[],
            )

        monkeypatch.setattr("kelp.config.init", _fake_init)
        monkeypatch.setattr("kelp.config.get_context", _fake_get_context)
        monkeypatch.setattr(
            "kelp.service.yaml_manager.YamlManager.patch_model_yaml",
            _fake_patch_model_yaml,
        )

        import_contract(
            source=FIXTURES_DIR / "orders_contract.yml",
            patch=True,
            dry_run=True,
        )

        output = capsys.readouterr().out
        assert len(called_models) == 1
        assert called_models[0].name == "orders"
        assert f"would patch: orders -> {tmp_path / 'fake.yml'}" in output

    def test_import_cli_patch_uses_local_origin_file_path(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Import patch mode should use local model origin_file_path for patch destination."""
        from kelp.service.yaml_manager import YamlUpdateReport

        called_models: list[Model] = []

        class _CatalogIndexStub:
            def __init__(self, models_by_name: dict[str, Model]) -> None:
                self._models_by_name = models_by_name

            def get_index(self, name: str):
                if name == "models":
                    return self._models_by_name
                return {}

        class _ContextStub:
            def __init__(self, models_by_name: dict[str, Model]) -> None:
                self.catalog_index = _CatalogIndexStub(models_by_name)

        local_orders_model = Model(name="orders")
        local_orders_model.origin_file_path = "bronze/orders.yml"

        def _fake_init(**kwargs):
            return None

        def _fake_get_context():
            return _ContextStub({"orders": local_orders_model})

        def _fake_patch_model_yaml(model: Model, **kwargs):
            called_models.append(model)
            return YamlUpdateReport(
                model_name=model.name,
                file_path=tmp_path / "fake.yml",
                result_model={"name": model.name},
                changes_made=True,
                added_fields=["name"],
                updated_fields=[],
                removed_fields=[],
            )

        monkeypatch.setattr("kelp.config.init", _fake_init)
        monkeypatch.setattr("kelp.config.get_context", _fake_get_context)
        monkeypatch.setattr(
            "kelp.service.yaml_manager.YamlManager.patch_model_yaml",
            _fake_patch_model_yaml,
        )

        import_contract(
            source=FIXTURES_DIR / "orders_contract.yml",
            patch=True,
            dry_run=True,
        )

        assert len(called_models) == 1
        assert called_models[0].name == "orders"
        assert called_models[0].origin_file_path == "bronze/orders.yml"

    def test_export_cli_patch_updates_only_matching_schema(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        sample_kelp_model: Model,
    ) -> None:
        """Export patch should update matching schema and keep others untouched."""
        existing_contract = tmp_path / "contract.yml"
        existing_contract.write_text(
            """
kind: DataContract
apiVersion: v3.1.0
name: Existing Contract
version: 1.0.0
schema:
  - name: customers
    physicalType: table
    customProperties:
      - property: keep_me
        value: yes
    properties:
      - name: customer_id
        physicalType: string
      - name: legacy_only
        physicalType: string
  - name: orders
    physicalType: table
    description: do-not-touch
""".strip(),
            encoding="utf-8",
        )

        def _fake_init(**kwargs):
            return SimpleNamespace(catalog={"models": [sample_kelp_model]})

        monkeypatch.setattr("kelp.config.init", _fake_init)

        export_contract(
            model="customers",
            patch=True,
            contract_file=existing_contract,
        )

        patched = yaml.safe_load(existing_contract.read_text(encoding="utf-8"))
        schemas = patched.get("schema", [])
        assert len(schemas) == 2

        customers = next(schema for schema in schemas if schema.get("name") == "customers")
        orders = next(schema for schema in schemas if schema.get("name") == "orders")

        # Ensure target schema updated while preserving local-only fields
        customer_props = {prop.get("name") for prop in customers.get("properties", [])}
        assert "customer_id" in customer_props
        assert "legacy_only" in customer_props
        custom_props = {
            item.get("property"): item.get("value")
            for item in customers.get("customProperties", [])
        }
        assert custom_props.get("keep_me") is True

        # Ensure non-target schema remains untouched
        assert orders.get("description") == "do-not-touch"
