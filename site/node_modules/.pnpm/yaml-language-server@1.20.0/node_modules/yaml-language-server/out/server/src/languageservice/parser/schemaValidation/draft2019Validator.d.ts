import type { JSONSchema } from '../../jsonSchema';
import { SchemaDialect } from '../../jsonSchema';
import type { ASTNode, ArrayASTNode, ObjectASTNode } from '../../jsonASTTypes';
import { Draft07Validator } from './draft07Validator';
import { ValidationResult } from './baseValidator';
import type { ISchemaCollector, Options } from './baseValidator';
export declare class Draft2019Validator extends Draft07Validator {
    protected getCurrentDialect(): SchemaDialect;
    /**
     * Keyword: contains + minContains/maxContains
     *
     * Draft-07 behavior: contains must match at least 1 item.
     * Draft-2019-09 behavior: minContains/maxContains constrain how many matches are required/allowed.
     */
    protected applyContains(node: ArrayASTNode, schema: JSONSchema, originalSchema: JSONSchema, validationResult: ValidationResult, _matchingSchemas: ISchemaCollector, options: Options): void;
    /**
     * Keyword: dependentRequired + dependentSchemas.
     */
    protected applyDependencies(node: ObjectASTNode, schema: JSONSchema, originalSchema: JSONSchema, validationResult: ValidationResult, matchingSchemas: ISchemaCollector, options: Options, seenKeys: Record<string, ASTNode>): void;
    /**
     * Keyword: unevaluatedProperties
     */
    protected applyUnevaluatedProperties(node: ObjectASTNode, schema: JSONSchema, originalSchema: JSONSchema, validationResult: ValidationResult, matchingSchemas: ISchemaCollector, options: Options, seenKeys?: Record<string, ASTNode>): void;
    /**
     * Keyword: unevaluatedItems
     */
    protected applyUnevaluatedItems(node: ArrayASTNode, schema: JSONSchema, originalSchema: JSONSchema, validationResult: ValidationResult, matchingSchemas: ISchemaCollector, options: Options): void;
}
