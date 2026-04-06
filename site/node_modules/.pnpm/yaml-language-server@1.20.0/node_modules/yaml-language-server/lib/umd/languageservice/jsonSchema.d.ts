import { CompletionItemKind } from 'vscode-json-languageservice';
import { SchemaVersions } from './yamlTypes';
export type JSONSchemaRef = JSONSchema | boolean;
export declare enum SchemaDialect {
    draft04 = "draft04",
    draft07 = "draft07",
    draft2019 = "draft2019-09",
    draft2020 = "draft2020-12"
}
export interface JSONSchema {
    _dialect?: SchemaDialect;
    _baseUrl?: string;
    _$ref?: string;
    id?: string;
    $id?: string;
    $schema?: string;
    url?: string;
    type?: string | string[];
    title?: string;
    closestTitle?: string;
    versions?: SchemaVersions;
    default?: any;
    definitions?: {
        [name: string]: JSONSchema;
    };
    description?: string;
    properties?: JSONSchemaMap;
    patternProperties?: JSONSchemaMap;
    additionalProperties?: JSONSchemaRef;
    minProperties?: number;
    maxProperties?: number;
    dependencies?: JSONSchemaMap | {
        [prop: string]: string[];
    };
    items?: JSONSchemaRef | JSONSchemaRef[];
    minItems?: number;
    maxItems?: number;
    uniqueItems?: boolean;
    additionalItems?: JSONSchemaRef;
    pattern?: string;
    minLength?: number;
    maxLength?: number;
    minimum?: number;
    maximum?: number;
    exclusiveMinimum?: boolean | number;
    exclusiveMaximum?: boolean | number;
    multipleOf?: number;
    required?: string[];
    $ref?: string;
    anyOf?: JSONSchemaRef[];
    allOf?: JSONSchemaRef[];
    oneOf?: JSONSchemaRef[];
    not?: JSONSchemaRef;
    enum?: any[];
    format?: string;
    const?: any;
    contains?: JSONSchemaRef;
    propertyNames?: JSONSchemaRef;
    examples?: any[];
    $comment?: string;
    if?: JSONSchemaRef;
    then?: JSONSchemaRef;
    else?: JSONSchemaRef;
    $anchor?: string;
    $defs?: {
        [name: string]: JSONSchema;
    };
    $recursiveAnchor?: boolean;
    $recursiveRef?: string;
    $vocabulary?: Record<string, boolean>;
    dependentSchemas?: JSONSchemaMap;
    unevaluatedItems?: JSONSchemaRef;
    unevaluatedProperties?: JSONSchemaRef;
    dependentRequired?: Record<string, string[]>;
    minContains?: number;
    maxContains?: number;
    prefixItems?: JSONSchemaRef[];
    $dynamicRef?: string;
    $dynamicAnchor?: string;
    defaultSnippets?: {
        label?: string;
        description?: string;
        markdownDescription?: string;
        type?: string;
        suggestionKind?: CompletionItemKind;
        sortText?: string;
        body?: any;
        bodyText?: string;
    }[];
    errorMessage?: string;
    patternErrorMessage?: string;
    deprecationMessage?: string;
    enumDescriptions?: string[];
    markdownEnumDescriptions?: string[];
    markdownDescription?: string;
    doNotSuggest?: boolean;
    allowComments?: boolean;
    schemaSequence?: JSONSchema[];
    filePatternAssociation?: string;
}
export interface JSONSchemaMap {
    [name: string]: JSONSchemaRef;
}
