import type { JSONSchema } from '../../jsonSchema';
import { SchemaDialect } from '../../jsonSchema';
import type { ArrayASTNode } from '../../jsonASTTypes';
import { Draft2019Validator } from './draft2019Validator';
import type { ISchemaCollector, Options } from './baseValidator';
import { ValidationResult } from './baseValidator';
export declare class Draft2020Validator extends Draft2019Validator {
    protected getCurrentDialect(): SchemaDialect;
    /**
     * Keyword: prefixItems + items
     */
    protected validateArrayNode(node: ArrayASTNode, schema: JSONSchema, originalSchema: JSONSchema, validationResult: ValidationResult, matchingSchemas: ISchemaCollector, options: Options): void;
    /**
     * Draft 2020-12: contains keyword affects the unevaluatedItems keyword
     */
    protected applyContains(node: ArrayASTNode, schema: JSONSchema, originalSchema: JSONSchema, validationResult: ValidationResult, _matchingSchemas: ISchemaCollector, options: Options): void;
}
