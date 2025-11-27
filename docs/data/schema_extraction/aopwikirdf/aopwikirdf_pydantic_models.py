"""
Pydantic models generated from LinkML schema for aopwikirdf

Generated from: aopwikirdf_linkml_schema.yaml
"""

from __future__ import annotations

import re
import sys
from datetime import (
    date,
    datetime,
    time
)
from decimal import Decimal
from enum import Enum
from typing import (
    Any,
    ClassVar,
    Literal,
    Optional,
    Union
)

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    RootModel,
    SerializationInfo,
    SerializerFunctionWrapHandler,
    field_validator,
    model_serializer
)


metamodel_version = "1.7.0"
version = "None"


class ConfiguredBaseModel(BaseModel):
    model_config = ConfigDict(
        serialize_by_alias = True,
        validate_by_name = True,
        validate_assignment = True,
        validate_default = True,
        extra = "forbid",
        arbitrary_types_allowed = True,
        use_enum_values = True,
        strict = False,
    )

    @model_serializer(mode='wrap', when_used='unless-none')
    def treat_empty_lists_as_none(
            self, handler: SerializerFunctionWrapHandler,
            info: SerializationInfo) -> dict[str, Any]:
        if info.exclude_none:
            _instance = self.model_copy()
            for field, field_info in type(_instance).model_fields.items():
                if getattr(_instance, field) == [] and not(
                        field_info.is_required()):
                    setattr(_instance, field, None)
        else:
            _instance = self
        return handler(_instance, info)



class LinkMLMeta(RootModel):
    root: dict[str, Any] = {}
    model_config = ConfigDict(frozen=True)

    def __getattr__(self, key:str):
        return getattr(self.root, key)

    def __getitem__(self, key:str):
        return self.root[key]

    def __setitem__(self, key:str, value):
        self.root[key] = value

    def __contains__(self, key:str) -> bool:
        return key in self.root


linkml_meta = LinkMLMeta({'default_prefix': 'aopwikirdf_schema',
     'default_range': 'string',
     'description': 'LinkML schema for aopwikirdf generated from JSON-LD',
     'generation_date': '2025-11-26T13:52:00',
     'id': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/',
     'name': 'aopwikirdf_schema',
     'prefixes': {'aopo': {'prefix_prefix': 'aopo',
                           'prefix_reference': 'http://aopkb.org/aop_ontology#'},
                  'aopwikirdf_schema': {'prefix_prefix': 'aopwikirdf_schema',
                                        'prefix_reference': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/'},
                  'cheminf': {'prefix_prefix': 'cheminf',
                              'prefix_reference': 'http://semanticscience.org/resource/'},
                  'dc': {'prefix_prefix': 'dc',
                         'prefix_reference': 'http://purl.org/dc/elements/1.1/'},
                  'dcat': {'prefix_prefix': 'dcat',
                           'prefix_reference': 'http://www.w3.org/ns/dcat#'},
                  'dcterms': {'prefix_prefix': 'dcterms',
                              'prefix_reference': 'http://purl.org/dc/terms/'},
                  'edam.data': {'prefix_prefix': 'edam.data',
                                'prefix_reference': 'http://edamontology.org/'},
                  'foaf': {'prefix_prefix': 'foaf',
                           'prefix_reference': 'http://xmlns.com/foaf/0.1/'},
                  'go': {'prefix_prefix': 'go',
                         'prefix_reference': 'http://purl.obolibrary.org/obo/'},
                  'linkml': {'prefix_prefix': 'linkml',
                             'prefix_reference': 'https://w3id.org/linkml/'},
                  'ncbitaxon': {'prefix_prefix': 'ncbitaxon',
                                'prefix_reference': 'http://purl.bioontology.org/ontology/NCBITAXON/'},
                  'ncit': {'prefix_prefix': 'ncit',
                           'prefix_reference': 'http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#'},
                  'pato': {'prefix_prefix': 'pato',
                           'prefix_reference': 'http://purl.obolibrary.org/obo/'},
                  'rdf': {'prefix_prefix': 'rdf',
                          'prefix_reference': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#'},
                  'rdfs': {'prefix_prefix': 'rdfs',
                           'prefix_reference': 'http://www.w3.org/2000/01/rdf-schema#'},
                  'schema': {'prefix_prefix': 'schema',
                             'prefix_reference': 'http://schema.org/'},
                  'skos': {'prefix_prefix': 'skos',
                           'prefix_reference': 'http://www.w3.org/2004/02/skos/core#'},
                  'void': {'prefix_prefix': 'void',
                           'prefix_reference': 'http://rdfs.org/ns/void#'},
                  'xsd': {'prefix_prefix': 'xsd',
                          'prefix_reference': 'http://www.w3.org/2001/XMLSchema#'}},
     'source_file': '/home/javi/rdfsolve-1/notebooks/02_pydantic_models/../../docs/data/schema_extraction/aopwikirdf/aopwikirdf_linkml_schema.yaml',
     'types': {'string': {'base': 'str',
                          'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/String',
                          'from_schema': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/',
                          'name': 'string',
                          'uri': 'xsd:string'},
               'uriorcurie': {'base': 'URIorCURIE',
                              'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/Uriorcurie',
                              'from_schema': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/',
                              'name': 'uriorcurie',
                              'uri': 'xsd:anyURI'}}} )


class Cheminf000000(ConfiguredBaseModel):
    """
    Class representing cheminf_000000
    """
    linkml_meta: ClassVar[LinkMLMeta] = LinkMLMeta({'class_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/cheminf_000000',
         'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/Cheminf000000',
         'from_schema': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/',
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/cheminf_000000']})

    skos_exact_Match: Optional[EdamData2291] = Field(default=None, description="""Property skos:exactMatch""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/skos_exact_Match',
         'domain_of': ['edam_data_1025',
                       'cheminf_000446',
                       'edam_data_2298',
                       'cheminf_000000',
                       'pato_0001241'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/skos_exact_Match'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/skos_exact_Match'} })
    dc_identifier: Optional[Ncbitaxon131567] = Field(default=None, description="""Property dc:identifier""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dc_identifier',
         'domain_of': ['aopo_Key_Event_Relationship',
                       'aopo_Key_Event',
                       'aopo_Adverse_Outcome_Pathway',
                       'ncit_C_54571',
                       'cheminf_000446',
                       'cheminf_000000',
                       'pato_0001241',
                       'go_0008150',
                       'aopo_Organ_Context',
                       'aopo_Cell_Type_Context',
                       'ncbitaxon_131567'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dc_identifier'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dc_identifier'} })
    dcterms_is_Part_Of: Optional[NcitC54571] = Field(default=None, description="""Property dcterms:isPartOf""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dcterms_is_Part_Of',
         'domain_of': ['aopo_Key_Event_Relationship',
                       'aopo_Key_Event',
                       'ncit_C_54571',
                       'cheminf_000446',
                       'cheminf_000000'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dcterms_is_Part_Of'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dcterms_is_Part_Of'} })


class Ncbitaxon131567(ConfiguredBaseModel):
    """
    Class representing ncbitaxon_131567
    """
    linkml_meta: ClassVar[LinkMLMeta] = LinkMLMeta({'class_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/ncbitaxon_131567',
         'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/Ncbitaxon131567',
         'from_schema': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/',
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/ncbitaxon_131567']})

    dc_identifier: Optional[Ncbitaxon131567] = Field(default=None, description="""Property dc:identifier""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dc_identifier',
         'domain_of': ['aopo_Key_Event_Relationship',
                       'aopo_Key_Event',
                       'aopo_Adverse_Outcome_Pathway',
                       'ncit_C_54571',
                       'cheminf_000446',
                       'cheminf_000000',
                       'pato_0001241',
                       'go_0008150',
                       'aopo_Organ_Context',
                       'aopo_Cell_Type_Context',
                       'ncbitaxon_131567'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dc_identifier'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dc_identifier'} })


class Go0008150(ConfiguredBaseModel):
    """
    Class representing go_0008150
    """
    linkml_meta: ClassVar[LinkMLMeta] = LinkMLMeta({'class_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/go_0008150',
         'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/Go0008150',
         'from_schema': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/',
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/go_0008150']})

    dc_identifier: Optional[Ncbitaxon131567] = Field(default=None, description="""Property dc:identifier""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dc_identifier',
         'domain_of': ['aopo_Key_Event_Relationship',
                       'aopo_Key_Event',
                       'aopo_Adverse_Outcome_Pathway',
                       'ncit_C_54571',
                       'cheminf_000446',
                       'cheminf_000000',
                       'pato_0001241',
                       'go_0008150',
                       'aopo_Organ_Context',
                       'aopo_Cell_Type_Context',
                       'ncbitaxon_131567'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dc_identifier'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dc_identifier'} })


class AopoKeyEventRelationship(ConfiguredBaseModel):
    """
    Class representing aopo_Key_Event_Relationship
    """
    linkml_meta: ClassVar[LinkMLMeta] = LinkMLMeta({'class_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/aopo_Key_Event_Relationship',
         'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/AopoKeyEventRelationship',
         'from_schema': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/',
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/aopo_Key_Event_Relationship']})

    foaf_page: Optional[NcitC54571] = Field(default=None, description="""Property foaf:page""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/foaf_page',
         'domain_of': ['aopo_Key_Event_Relationship',
                       'aopo_Key_Event',
                       'aopo_Adverse_Outcome_Pathway',
                       'ncit_C_54571'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/foaf_page'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/foaf_page'} })
    dc_identifier: Optional[Ncbitaxon131567] = Field(default=None, description="""Property dc:identifier""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dc_identifier',
         'domain_of': ['aopo_Key_Event_Relationship',
                       'aopo_Key_Event',
                       'aopo_Adverse_Outcome_Pathway',
                       'ncit_C_54571',
                       'cheminf_000446',
                       'cheminf_000000',
                       'pato_0001241',
                       'go_0008150',
                       'aopo_Organ_Context',
                       'aopo_Cell_Type_Context',
                       'ncbitaxon_131567'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dc_identifier'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dc_identifier'} })
    dcterms_is_Part_Of: Optional[NcitC54571] = Field(default=None, description="""Property dcterms:isPartOf""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dcterms_is_Part_Of',
         'domain_of': ['aopo_Key_Event_Relationship',
                       'aopo_Key_Event',
                       'ncit_C_54571',
                       'cheminf_000446',
                       'cheminf_000000'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dcterms_is_Part_Of'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dcterms_is_Part_Of'} })
    has_edam_data_1025: Optional[EdamData2298] = Field(default=None, description="""Property edam.data:1025""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/has_edam_data_1025',
         'domain_of': ['aopo_Key_Event_Relationship', 'aopo_Key_Event'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/has_edam_data_1025'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/has_edam_data_1025'} })
    aopo_has_downstream_key_event: Optional[AopoKeyEvent] = Field(default=None, description="""Property aopo:has_downstream_key_event""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/aopo_has_downstream_key_event',
         'domain_of': ['aopo_Key_Event_Relationship'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/aopo_has_downstream_key_event'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/aopo_has_downstream_key_event'} })
    rdfs_see_Also: Optional[AopoAdverseOutcomePathway] = Field(default=None, description="""Property rdfs:seeAlso""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/rdfs_see_Also',
         'domain_of': ['aopo_Key_Event_Relationship',
                       'aopo_Key_Event',
                       'aopo_Adverse_Outcome_Pathway'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/rdfs_see_Also'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/rdfs_see_Also'} })
    aopo_has_upstream_key_event: Optional[AopoKeyEvent] = Field(default=None, description="""Property aopo:has_upstream_key_event""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/aopo_has_upstream_key_event',
         'domain_of': ['aopo_Key_Event_Relationship'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/aopo_has_upstream_key_event'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/aopo_has_upstream_key_event'} })


class AopoKeyEvent(ConfiguredBaseModel):
    """
    Class representing aopo_Key_Event
    """
    linkml_meta: ClassVar[LinkMLMeta] = LinkMLMeta({'class_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/aopo_Key_Event',
         'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/AopoKeyEvent',
         'from_schema': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/',
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/aopo_Key_Event']})

    foaf_page: Optional[NcitC54571] = Field(default=None, description="""Property foaf:page""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/foaf_page',
         'domain_of': ['aopo_Key_Event_Relationship',
                       'aopo_Key_Event',
                       'aopo_Adverse_Outcome_Pathway',
                       'ncit_C_54571'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/foaf_page'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/foaf_page'} })
    dc_identifier: Optional[Ncbitaxon131567] = Field(default=None, description="""Property dc:identifier""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dc_identifier',
         'domain_of': ['aopo_Key_Event_Relationship',
                       'aopo_Key_Event',
                       'aopo_Adverse_Outcome_Pathway',
                       'ncit_C_54571',
                       'cheminf_000446',
                       'cheminf_000000',
                       'pato_0001241',
                       'go_0008150',
                       'aopo_Organ_Context',
                       'aopo_Cell_Type_Context',
                       'ncbitaxon_131567'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dc_identifier'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dc_identifier'} })
    dcterms_is_Part_Of: Optional[NcitC54571] = Field(default=None, description="""Property dcterms:isPartOf""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dcterms_is_Part_Of',
         'domain_of': ['aopo_Key_Event_Relationship',
                       'aopo_Key_Event',
                       'ncit_C_54571',
                       'cheminf_000446',
                       'cheminf_000000'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dcterms_is_Part_Of'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dcterms_is_Part_Of'} })
    has_pato_0001241: Optional[Go0008150] = Field(default=None, description="""Property pato:0001241""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/has_pato_0001241',
         'domain_of': ['aopo_Key_Event'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/has_pato_0001241'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/has_pato_0001241'} })
    aopo_has_Biological_Event: Optional[AopoBiologicalEvent] = Field(default=None, description="""Property aopo:hasBiologicalEvent""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/aopo_has_Biological_Event',
         'domain_of': ['aopo_Key_Event'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/aopo_has_Biological_Event'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/aopo_has_Biological_Event'} })
    has_edam_data_1025: Optional[EdamData2298] = Field(default=None, description="""Property edam.data:1025""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/has_edam_data_1025',
         'domain_of': ['aopo_Key_Event_Relationship', 'aopo_Key_Event'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/has_edam_data_1025'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/has_edam_data_1025'} })
    rdfs_see_Also: Optional[AopoAdverseOutcomePathway] = Field(default=None, description="""Property rdfs:seeAlso""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/rdfs_see_Also',
         'domain_of': ['aopo_Key_Event_Relationship',
                       'aopo_Key_Event',
                       'aopo_Adverse_Outcome_Pathway'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/rdfs_see_Also'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/rdfs_see_Also'} })
    has_aopo_Organ_Context: Optional[Pato0001241] = Field(default=None, description="""Property aopo:OrganContext""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/has_aopo_Organ_Context',
         'domain_of': ['aopo_Key_Event'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/has_aopo_Organ_Context'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/has_aopo_Organ_Context'} })
    has_aopo_Cell_Type_Context: Optional[Pato0001241] = Field(default=None, description="""Property aopo:CellTypeContext""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/has_aopo_Cell_Type_Context',
         'domain_of': ['aopo_Key_Event'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/has_aopo_Cell_Type_Context'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/has_aopo_Cell_Type_Context'} })
    has_go_0008150: Optional[Pato0001241] = Field(default=None, description="""Property go:0008150""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/has_go_0008150',
         'domain_of': ['aopo_Key_Event'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/has_go_0008150'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/has_go_0008150'} })
    has_ncbitaxon_131567: Optional[Ncbitaxon131567] = Field(default=None, description="""Property ncbitaxon:131567""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/has_ncbitaxon_131567',
         'domain_of': ['aopo_Key_Event'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/has_ncbitaxon_131567'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/has_ncbitaxon_131567'} })


class Cheminf000405(ConfiguredBaseModel):
    """
    Class representing cheminf_000405
    """
    linkml_meta: ClassVar[LinkMLMeta] = LinkMLMeta({'class_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/cheminf_000405',
         'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/Cheminf000405',
         'from_schema': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/',
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/cheminf_000405']})

    pass


class Pato0001241(ConfiguredBaseModel):
    """
    Class representing pato_0001241
    """
    linkml_meta: ClassVar[LinkMLMeta] = LinkMLMeta({'class_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/pato_0001241',
         'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/Pato0001241',
         'from_schema': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/',
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/pato_0001241']})

    skos_exact_Match: Optional[EdamData2291] = Field(default=None, description="""Property skos:exactMatch""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/skos_exact_Match',
         'domain_of': ['edam_data_1025',
                       'cheminf_000446',
                       'edam_data_2298',
                       'cheminf_000000',
                       'pato_0001241'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/skos_exact_Match'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/skos_exact_Match'} })
    dc_identifier: Optional[Ncbitaxon131567] = Field(default=None, description="""Property dc:identifier""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dc_identifier',
         'domain_of': ['aopo_Key_Event_Relationship',
                       'aopo_Key_Event',
                       'aopo_Adverse_Outcome_Pathway',
                       'ncit_C_54571',
                       'cheminf_000446',
                       'cheminf_000000',
                       'pato_0001241',
                       'go_0008150',
                       'aopo_Organ_Context',
                       'aopo_Cell_Type_Context',
                       'ncbitaxon_131567'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dc_identifier'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dc_identifier'} })


class VoidLinkset(ConfiguredBaseModel):
    """
    Class representing void_Linkset
    """
    linkml_meta: ClassVar[LinkMLMeta] = LinkMLMeta({'class_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/void_Linkset',
         'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/VoidLinkset',
         'from_schema': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/',
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/void_Linkset']})

    dcat_download_URL: Optional[VoidDataset] = Field(default=None, description="""Property dcat:downloadURL""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dcat_download_URL',
         'domain_of': ['void_Linkset', 'void_Dataset'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dcat_download_URL'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dcat_download_URL'} })


class EdamData1025(ConfiguredBaseModel):
    """
    Class representing edam_data_1025
    """
    linkml_meta: ClassVar[LinkMLMeta] = LinkMLMeta({'class_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/edam_data_1025',
         'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/EdamData1025',
         'from_schema': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/',
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/edam_data_1025']})

    skos_exact_Match: Optional[EdamData2291] = Field(default=None, description="""Property skos:exactMatch""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/skos_exact_Match',
         'domain_of': ['edam_data_1025',
                       'cheminf_000446',
                       'edam_data_2298',
                       'cheminf_000000',
                       'pato_0001241'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/skos_exact_Match'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/skos_exact_Match'} })


class AopoAdverseOutcomePathway(ConfiguredBaseModel):
    """
    Class representing aopo_Adverse_Outcome_Pathway
    """
    linkml_meta: ClassVar[LinkMLMeta] = LinkMLMeta({'class_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/aopo_Adverse_Outcome_Pathway',
         'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/AopoAdverseOutcomePathway',
         'from_schema': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/',
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/aopo_Adverse_Outcome_Pathway']})

    foaf_page: Optional[NcitC54571] = Field(default=None, description="""Property foaf:page""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/foaf_page',
         'domain_of': ['aopo_Key_Event_Relationship',
                       'aopo_Key_Event',
                       'aopo_Adverse_Outcome_Pathway',
                       'ncit_C_54571'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/foaf_page'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/foaf_page'} })
    has_ncit_C_54571: Optional[NcitC54571] = Field(default=None, description="""Property ncit:C54571""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/has_ncit_C_54571',
         'domain_of': ['aopo_Adverse_Outcome_Pathway'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/has_ncit_C_54571'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/has_ncit_C_54571'} })
    dc_identifier: Optional[Ncbitaxon131567] = Field(default=None, description="""Property dc:identifier""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dc_identifier',
         'domain_of': ['aopo_Key_Event_Relationship',
                       'aopo_Key_Event',
                       'aopo_Adverse_Outcome_Pathway',
                       'ncit_C_54571',
                       'cheminf_000446',
                       'cheminf_000000',
                       'pato_0001241',
                       'go_0008150',
                       'aopo_Organ_Context',
                       'aopo_Cell_Type_Context',
                       'ncbitaxon_131567'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dc_identifier'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dc_identifier'} })
    rdfs_see_Also: Optional[AopoAdverseOutcomePathway] = Field(default=None, description="""Property rdfs:seeAlso""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/rdfs_see_Also',
         'domain_of': ['aopo_Key_Event_Relationship',
                       'aopo_Key_Event',
                       'aopo_Adverse_Outcome_Pathway'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/rdfs_see_Also'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/rdfs_see_Also'} })
    aopo_has_adverse_outcome: Optional[AopoKeyEvent] = Field(default=None, description="""Property aopo:has_adverse_outcome""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/aopo_has_adverse_outcome',
         'domain_of': ['aopo_Adverse_Outcome_Pathway'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/aopo_has_adverse_outcome'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/aopo_has_adverse_outcome'} })
    aopo_has_key_event: Optional[AopoKeyEvent] = Field(default=None, description="""Property aopo:has_key_event""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/aopo_has_key_event',
         'domain_of': ['aopo_Adverse_Outcome_Pathway'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/aopo_has_key_event'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/aopo_has_key_event'} })
    aopo_has_key_event_relationship: Optional[AopoKeyEventRelationship] = Field(default=None, description="""Property aopo:has_key_event_relationship""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/aopo_has_key_event_relationship',
         'domain_of': ['aopo_Adverse_Outcome_Pathway'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/aopo_has_key_event_relationship'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/aopo_has_key_event_relationship'} })
    aopo_has_molecular_initiating_event: Optional[AopoKeyEvent] = Field(default=None, description="""Property aopo:has_molecular_initiating_event""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/aopo_has_molecular_initiating_event',
         'domain_of': ['aopo_Adverse_Outcome_Pathway'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/aopo_has_molecular_initiating_event'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/aopo_has_molecular_initiating_event'} })


class EdamData1027(ConfiguredBaseModel):
    """
    Class representing edam_data_1027
    """
    linkml_meta: ClassVar[LinkMLMeta] = LinkMLMeta({'class_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/edam_data_1027',
         'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/EdamData1027',
         'from_schema': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/',
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/edam_data_1027']})

    pass


class AopoBiologicalEvent(ConfiguredBaseModel):
    """
    Class representing aopo_Biological_Event
    """
    linkml_meta: ClassVar[LinkMLMeta] = LinkMLMeta({'class_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/aopo_Biological_Event',
         'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/AopoBiologicalEvent',
         'from_schema': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/',
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/aopo_Biological_Event']})

    aopo_has_Object: Optional[Go0008150] = Field(default=None, description="""Property aopo:hasObject""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/aopo_has_Object',
         'domain_of': ['aopo_Biological_Event'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/aopo_has_Object'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/aopo_has_Object'} })
    aopo_has_Process: Optional[Pato0001241] = Field(default=None, description="""Property aopo:hasProcess""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/aopo_has_Process',
         'domain_of': ['aopo_Biological_Event'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/aopo_has_Process'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/aopo_has_Process'} })


class EdamData2291(ConfiguredBaseModel):
    """
    Class representing edam_data_2291
    """
    linkml_meta: ClassVar[LinkMLMeta] = LinkMLMeta({'class_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/edam_data_2291',
         'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/EdamData2291',
         'from_schema': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/',
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/edam_data_2291']})

    pass


class VoidDataset(ConfiguredBaseModel):
    """
    Class representing void_Dataset
    """
    linkml_meta: ClassVar[LinkMLMeta] = LinkMLMeta({'class_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/void_Dataset',
         'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/VoidDataset',
         'from_schema': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/',
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/void_Dataset']})

    dcat_download_URL: Optional[VoidDataset] = Field(default=None, description="""Property dcat:downloadURL""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dcat_download_URL',
         'domain_of': ['void_Linkset', 'void_Dataset'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dcat_download_URL'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dcat_download_URL'} })


class NcitC54571(ConfiguredBaseModel):
    """
    Class representing ncit_C_54571
    """
    linkml_meta: ClassVar[LinkMLMeta] = LinkMLMeta({'class_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/ncit_C_54571',
         'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/NcitC54571',
         'from_schema': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/',
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/ncit_C_54571']})

    foaf_page: Optional[NcitC54571] = Field(default=None, description="""Property foaf:page""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/foaf_page',
         'domain_of': ['aopo_Key_Event_Relationship',
                       'aopo_Key_Event',
                       'aopo_Adverse_Outcome_Pathway',
                       'ncit_C_54571'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/foaf_page'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/foaf_page'} })
    dc_identifier: Optional[Ncbitaxon131567] = Field(default=None, description="""Property dc:identifier""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dc_identifier',
         'domain_of': ['aopo_Key_Event_Relationship',
                       'aopo_Key_Event',
                       'aopo_Adverse_Outcome_Pathway',
                       'ncit_C_54571',
                       'cheminf_000446',
                       'cheminf_000000',
                       'pato_0001241',
                       'go_0008150',
                       'aopo_Organ_Context',
                       'aopo_Cell_Type_Context',
                       'ncbitaxon_131567'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dc_identifier'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dc_identifier'} })
    dcterms_is_Part_Of: Optional[NcitC54571] = Field(default=None, description="""Property dcterms:isPartOf""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dcterms_is_Part_Of',
         'domain_of': ['aopo_Key_Event_Relationship',
                       'aopo_Key_Event',
                       'ncit_C_54571',
                       'cheminf_000446',
                       'cheminf_000000'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dcterms_is_Part_Of'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dcterms_is_Part_Of'} })
    aopo_has_chemical_entity: Optional[Cheminf000446] = Field(default=None, description="""Property aopo:has_chemical_entity""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/aopo_has_chemical_entity',
         'domain_of': ['ncit_C_54571'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/aopo_has_chemical_entity'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/aopo_has_chemical_entity'} })


class AopoCellTypeContext(ConfiguredBaseModel):
    """
    Class representing aopo_Cell_Type_Context
    """
    linkml_meta: ClassVar[LinkMLMeta] = LinkMLMeta({'class_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/aopo_Cell_Type_Context',
         'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/AopoCellTypeContext',
         'from_schema': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/',
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/aopo_Cell_Type_Context']})

    dc_identifier: Optional[Ncbitaxon131567] = Field(default=None, description="""Property dc:identifier""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dc_identifier',
         'domain_of': ['aopo_Key_Event_Relationship',
                       'aopo_Key_Event',
                       'aopo_Adverse_Outcome_Pathway',
                       'ncit_C_54571',
                       'cheminf_000446',
                       'cheminf_000000',
                       'pato_0001241',
                       'go_0008150',
                       'aopo_Organ_Context',
                       'aopo_Cell_Type_Context',
                       'ncbitaxon_131567'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dc_identifier'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dc_identifier'} })


class Cheminf000446(ConfiguredBaseModel):
    """
    Class representing cheminf_000446
    """
    linkml_meta: ClassVar[LinkMLMeta] = LinkMLMeta({'class_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/cheminf_000446',
         'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/Cheminf000446',
         'from_schema': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/',
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/cheminf_000446']})

    skos_exact_Match: Optional[EdamData2291] = Field(default=None, description="""Property skos:exactMatch""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/skos_exact_Match',
         'domain_of': ['edam_data_1025',
                       'cheminf_000446',
                       'edam_data_2298',
                       'cheminf_000000',
                       'pato_0001241'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/skos_exact_Match'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/skos_exact_Match'} })
    dc_identifier: Optional[Ncbitaxon131567] = Field(default=None, description="""Property dc:identifier""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dc_identifier',
         'domain_of': ['aopo_Key_Event_Relationship',
                       'aopo_Key_Event',
                       'aopo_Adverse_Outcome_Pathway',
                       'ncit_C_54571',
                       'cheminf_000446',
                       'cheminf_000000',
                       'pato_0001241',
                       'go_0008150',
                       'aopo_Organ_Context',
                       'aopo_Cell_Type_Context',
                       'ncbitaxon_131567'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dc_identifier'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dc_identifier'} })
    dcterms_is_Part_Of: Optional[NcitC54571] = Field(default=None, description="""Property dcterms:isPartOf""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dcterms_is_Part_Of',
         'domain_of': ['aopo_Key_Event_Relationship',
                       'aopo_Key_Event',
                       'ncit_C_54571',
                       'cheminf_000446',
                       'cheminf_000000'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dcterms_is_Part_Of'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dcterms_is_Part_Of'} })


class AopoOrganContext(ConfiguredBaseModel):
    """
    Class representing aopo_Organ_Context
    """
    linkml_meta: ClassVar[LinkMLMeta] = LinkMLMeta({'class_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/aopo_Organ_Context',
         'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/AopoOrganContext',
         'from_schema': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/',
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/aopo_Organ_Context']})

    dc_identifier: Optional[Ncbitaxon131567] = Field(default=None, description="""Property dc:identifier""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dc_identifier',
         'domain_of': ['aopo_Key_Event_Relationship',
                       'aopo_Key_Event',
                       'aopo_Adverse_Outcome_Pathway',
                       'ncit_C_54571',
                       'cheminf_000446',
                       'cheminf_000000',
                       'pato_0001241',
                       'go_0008150',
                       'aopo_Organ_Context',
                       'aopo_Cell_Type_Context',
                       'ncbitaxon_131567'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dc_identifier'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/dc_identifier'} })


class EdamData2298(ConfiguredBaseModel):
    """
    Class representing edam_data_2298
    """
    linkml_meta: ClassVar[LinkMLMeta] = LinkMLMeta({'class_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/edam_data_2298',
         'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/EdamData2298',
         'from_schema': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/',
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/edam_data_2298']})

    skos_exact_Match: Optional[EdamData2291] = Field(default=None, description="""Property skos:exactMatch""", json_schema_extra = { "linkml_meta": {'definition_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/skos_exact_Match',
         'domain_of': ['edam_data_1025',
                       'cheminf_000446',
                       'edam_data_2298',
                       'cheminf_000000',
                       'pato_0001241'],
         'mappings': ['http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/skos_exact_Match'],
         'slot_uri': 'http://jmillanacosta.github.io/rdfsolve/aopwikirdf/linkml/skos_exact_Match'} })


# Model rebuild
# see https://pydantic-docs.helpmanual.io/usage/models/#rebuilding-a-model
Cheminf000000.model_rebuild()
Ncbitaxon131567.model_rebuild()
Go0008150.model_rebuild()
AopoKeyEventRelationship.model_rebuild()
AopoKeyEvent.model_rebuild()
Cheminf000405.model_rebuild()
Pato0001241.model_rebuild()
VoidLinkset.model_rebuild()
EdamData1025.model_rebuild()
AopoAdverseOutcomePathway.model_rebuild()
EdamData1027.model_rebuild()
AopoBiologicalEvent.model_rebuild()
EdamData2291.model_rebuild()
VoidDataset.model_rebuild()
NcitC54571.model_rebuild()
AopoCellTypeContext.model_rebuild()
Cheminf000446.model_rebuild()
AopoOrganContext.model_rebuild()
EdamData2298.model_rebuild()
