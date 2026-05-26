# Open Data Contract Standard Integration

Kelp integrates with the Open Data Contract Standard (ODCS) to provide a seamless experience for users who want to leverage ODCS in their data management workflows. This integration allows users to easily import and export Kelp models and configurations using ODCS-compliant formats, ensuring compatibility and interoperability with other tools and platforms that support ODCS.

## Prerequisites

To use kelp cli with ODCS integration, you need to have the following installed:

You can install the DataContract CLI using pip:
`pip install datacontract-cli`

DQX if you want to use the DQX check generation:
`pip install databricks-labs-dqx[datacontract]`

## Import kelp models from an ODCS YAML file

```bash
kelp odcs import path/to/odcs_file.yaml --output path/to/kelp_model.yaml
```

??? "odcs_file.yaml"

    ```yaml
    domain: seller # Domain
    dataProduct: my quantum # Data product name
    version: 1.1.0 # Version (follows semantic versioning)
    status: active
    id: 53581432-6c55-4ba2-a65f-72344a91553a

    # Lots of information
    description:
      purpose: Views built on top of the seller tables.
      limitations: Data based on seller perspective, no buyer information
      usage: Predict sales over time
      authoritativeDefinitions:
        - type: privacy-statement
          url: https://example.com/gdpr.pdf
    tenant: ClimateQuantumInc

    kind: DataContract
    apiVersion: v3.1.0 # Standard version (follows semantic versioning)

    # Dataset, schema, and quality
    schema:
      - id: tbl_obj
        name: tbl
        physicalName: tbl_1
        physicalType: table
        businessName: Core Payment Metrics
        description: Provides core payment metrics
        authoritativeDefinitions:
          - url: https://catalog.data.gov/dataset/air-quality
            type: businessDefinition
          - url: https://youtu.be/jbY1BKFj9ec
            type: videoTutorial
        tags: ["finance", "payments", example_key:example_value]
        dataGranularityDescription: Aggregation on columns txn_ref_dt, pmt_txn_id
        # Schema-level relationships (for composite keys)
        relationships:
          - type: foreignKey
            from:
              - tbl.rcvr_id
              - tbl.rcvr_cntry_code
            to:
              - receivers.id
              - receivers.country_code
            customProperties:
              - property: description
                value: "Composite key linking to receivers table"
              - property: cardinality
                value: "many-to-one"
        properties:
          - id: txn_ref_dt_prop
            name: transaction_reference_date
            physicalName: txn_ref_dt
            primaryKey: false
            primaryKeyPosition: -1
            businessName: transaction reference date
            logicalType: date
            physicalType: date
            required: false
            description: Reference date for transaction
            partitioned: true
            partitionKeyPosition: 1
            criticalDataElement: false
            tags: []
            classification: public
            transformSourceObjects:
              - table_name_1
              - table_name_2
              - table_name_3
            transformLogic: sel t1.txn_dt as txn_ref_dt from table_name_1 as t1, table_name_2 as t2, table_name_3 as t3 where t1.txn_dt=date-3
            transformDescription: defines the logic in business terms; logic for dummies
            examples:
              - "2022-10-03"
              - "2020-01-28"
            customProperties:
              - property: anonymizationStrategy
                value: none
          - id: rcvr_id_prop
            name: rcvr_id
            primaryKey: true
            primaryKeyPosition: 1
            businessName: receiver id
            logicalType: string
            physicalType: string
            required: false
            description: A description for column rcvr_id.
            partitioned: false
            partitionKeyPosition: -1
            criticalDataElement: false
            tags: ["uid"]
            classification: restricted
            # Property-level relationship (from is implicit)
            relationships:
              - to: receivers.id
                type: foreignKey
                customProperties:
                  - property: description
                    value: "Links to receiver master data"
          - id: rcvr_cntry_code_prop
            name: rcvr_cntry_code
            primaryKey: false
            primaryKeyPosition: -1
            businessName: receiver country code
            logicalType: string
            physicalType: string
            required: false
            description: Country code
            partitioned: false
            partitionKeyPosition: -1
            criticalDataElement: false
            tags: []
            classification: public
            authoritativeDefinitions:
              - url: https://collibra.com/asset/742b358f-71a5-4ab1-bda4-dcdba9418c25
                type: businessDefinition
              - url: https://github.com/myorg/myrepo
                type: transformationImplementation
              - url: jdbc:postgresql://localhost:5432/adventureworks/tbl_1/rcvr_cntry_code
                type: implementation
            encryptedName: rcvr_cntry_code_encrypted
            quality:
              - metric: nullValues
                mustBe: 0
                description: column should not contain null values
                dimension: completeness
                type: library
                severity: error
                businessImpact: operational
                schedule: 0 20 * * *
                scheduler: cron
                customProperties:
                  - property: FIELD_NAME
                    value:
                  - property: COMPARE_TO
                    value:
                  - property: COMPARISON_TYPE
                    value: Greater than

    # Team
    team:
      name: my-team
      description: The team owning the data contract
      members:
        - username: daustin
          role: Owner
          description: Keeper of the grail
          dateIn: "2022-10-01"

    # Custom properties
    customProperties:
      - property: refRulesetName
        value: gcsc.ruleset.name
      - property: somePropertyName
        value: property_value
    ```

??? "kelp_model.yaml"

    ```yaml
    kelp_models:
      - table_type: managed
        name: tbl
        description: Provides core payment metrics
        partition_cols:
          - txn_ref_dt
        columns:
          - name: txn_ref_dt
            description: Reference date for transaction
            data_type: date
          - name: rcvr_id
            description: A description for column rcvr_id.
            data_type: string
            tags:
              uid: ""
          - name: rcvr_cntry_code
            description: Country code
            data_type: string
        constraints:
          - name: pk_tbl
            columns:
              - rcvr_id
          - name: fk_tbl_receivers
            columns:
              - rcvr_id
              - rcvr_cntry_code
            reference_table: receivers
            reference_columns:
              - id
              - country_code
          - name: fk_rcvr_id_receivers
            columns:
              - rcvr_id
            reference_table: receivers
            reference_columns:
              - id
        tags:
          finance: ""
          payments: ""
          example_key: example_value
        meta:
          odcs_domain: seller
          odcs_status: active
          odcs_id: 53581432-6c55-4ba2-a65f-72344a91553a
          odcs_version: 1.1.0
          refRulesetName: gcsc.ruleset.name
          somePropertyName: property_value
          odcs_owner: daustin
    ```

### Import with DQX check generation

Reference: https://databrickslabs.github.io/dqx/docs/guide/data_contract_quality_rules_generation/

You can utilize DQX to automatically generate data quality checks based on the quality requirements defined in the ODCS file. To do this, you need to have DQX installed and then use the `--generate-dqx-rules` flag when importing the ODCS file:

```bash
kelp odcs import path/to/odcs_file.yaml --output path/to/kelp_model.yaml --generate-dqx-rules
```

This command will add following DQX checks to the generated Kelp model:

```yaml
...
quality:
    engine: dqx
    checks:
    - check:
        function: has_valid_schema
        arguments:
          expected_schema: transaction_reference_date DATE, rcvr_id STRING, rcvr_cntry_code
            STRING
          strict: true
      name: tbl_schema_validation
      criticality: error
```


## Export kelp models to an ODCS YAML file

```bash
kelp odcs export bronze_customers --target dev --output path/to/odcs_file.yaml
```

??? "kelp_model.yaml"
    ```yaml
    kelp_models:
      - name: bronze_customers
        description: This is the bronze customers model
        columns:
        - name: country
          data_type: string
          description: Customer country as provided by source (may contain 'unknown').
          tags:
            data_classification: internal
            geo: country
        - name: first_name
          data_type: string
          description: Customer given name.
          tags:
            data_classification: pii
        - name: last_name
          data_type: string
          description: Customer family name.
          tags:
            data_classification: pii
        - name: user_id
          data_type: string
          description: Internal user identifier; used to link orders to customers.
          tags:
            business: customer_id
            data_classification: pii
        - name: _rescued_data
          data_type: string
          description: JSON blob of rescued/parsing errors from the bronze ingestion.
          tags:
            etl: _rescued
        quality:
          engine: sdp
          expect_all_or_fail:
            _rescued_data_is_null: _rescued_data IS NULL
    ```

??? "odcs_file.yaml"
    ```yaml
    apiVersion: v3.1.0
    id: my-data-contract
    name: bronze_customers
    status: draft
    schema:
      - name: bronze_customers
        physicalType: table
        description: This is the bronze customers model
        tags:
          - kelp_managed
          - domain:sales
          - owner:analytics-team
        logicalType: object
        physicalName: bronze_customers
        properties:
          - name: country
            physicalType: string
            description: Customer country as provided by source (may contain 'unknown').
            tags:
              - data_classification:internal
              - geo:country
            logicalType: string
            required: false
          - name: first_name
            physicalType: string
            description: Customer given name.
            tags:
              - data_classification:pii
            logicalType: string
            required: false
          - name: last_name
            physicalType: string
            description: Customer family name.
            tags:
              - data_classification:pii
            logicalType: string
            required: false
          - name: user_id
            physicalType: string
            description: Internal user identifier; used to link orders to customers.
            tags:
              - business:customer_id
              - data_classification:pii
            logicalType: string
            required: false
          - name: _rescued_data
            physicalType: string
            description: JSON blob of rescued/parsing errors from the bronze ingestion.
            tags:
              - etl:_rescued
            logicalType: string
            required: false
        quality:
          - name: kelp_sdp_quality
            type: custom
            engine: sdp
            implementation:
              expect_all_or_fail:
                _rescued_data_is_null: _rescued_data IS NULL
    ```


## Patching models and odcs files
You can also use the import/export commands to patch existing Kelp models or ODCS files by passing `--patch` flag. When this flag is used, Kelp will update the existing model or ODCS file with the new information provided in the input file, rather than creating a new model or ODCS file. 

### Patching an existing Kelp model with new information from an ODCS file

```bash
kelp odcs import path/to/odcs_file.yaml --patch 
```
Will use schema name to identify the model to be patched and update model fields.

### Patching an existing ODCS file with new information from a Kelp model

```bash
kelp odcs export bronze_customers --target dev --patch --output path/to/odcs_file.yaml
```

Will patch the existing ODCS file at `path/to/odcs_file.yaml` with new information from `bronze_customers` model. The existing ODCS file will be updated with new fields from the Kelp model while preserving any fields that are not present in the Kelp model.

## Reference
For further cli options and details, please refer to the Kelp CLI documentation: 
[Kelp CLI Documentation](../cli.md)

## Additional Resources

- [ODCS Specification](https://bitol-io.github.io/open-data-contract-standard/latest/)
- [datacontract-cli](https://github.com/datacontract/datacontract-cli)
- [DQX Data Contract Quality Rules Generation](https://databrickslabs.github.io/dqx/docs/guide/data_contract_quality_rules_generation/)
