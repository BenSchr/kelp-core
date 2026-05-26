"""
Implementation of custom exporter/importer to convert ODCS data contract from/to Kelp models.
Be aware that export and import perspectives are switched based on the CustomerExporter/CustomImporter class names.
The CustomExporter is used when importing from ODCS to Kelp, and the CustomImporter is used when exporting from Kelp to ODCS.
"""

from kelp.integration.odcs.contract_yaml_patcher import patch_contract_yaml_document

__all__ = ["patch_contract_yaml_document"]
