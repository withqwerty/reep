import type { JSONSchema, JSONSchemaRef, SchemaDialect } from '../../jsonSchema';
import type { ASTNode, ArrayASTNode, NumberASTNode, ObjectASTNode, StringASTNode } from '../../jsonASTTypes';
import { ErrorCode } from 'vscode-json-languageservice';
import { Diagnostic, DiagnosticSeverity } from 'vscode-languageserver-types';
import type { TextDocument } from 'vscode-languageserver-textdocument';
export declare const YAML_SOURCE = "YAML";
export interface IRange {
    offset: number;
    length: number;
}
export declare enum ProblemType {
    missingRequiredPropWarning = "missingRequiredPropWarning",
    typeMismatchWarning = "typeMismatchWarning",
    constWarning = "constWarning"
}
export declare const ProblemTypeMessages: Record<ProblemType, string>;
export interface IProblem {
    location: IRange;
    severity: DiagnosticSeverity;
    code?: ErrorCode;
    message: string;
    source?: string;
    problemType?: ProblemType;
    problemArgs?: string[];
    schemaUri?: string[];
    data?: Record<string, unknown>;
}
export interface IApplicableSchema {
    node: ASTNode;
    inverted?: boolean;
    schema: JSONSchema;
}
export interface ISchemaCollector {
    schemas: IApplicableSchema[];
    add(schema: IApplicableSchema): void;
    merge(other: ISchemaCollector): void;
    include(node: ASTNode): boolean;
    newSub(): ISchemaCollector;
}
export declare const formats: Record<string, {
    errorMessage: string;
    pattern: RegExp;
}>;
export declare class ValidationResult {
    problems: IProblem[];
    propertiesMatches: number;
    propertiesValueMatches: number;
    primaryValueMatches: number;
    enumValueMatch: boolean;
    enumValues: any[];
    /**
     * Optional bookkeeping for newer drafts (2019/2020).
     * BaseValidator only populates evaluatedProperties conservatively for object keywords it processes directly.
     */
    evaluatedProperties?: Set<string>;
    evaluatedItemsByNode?: Map<ASTNode, Set<number>>;
    constructor(isKubernetes: boolean);
    getEvaluatedItems(node: ASTNode): Set<number>;
    hasProblems(): boolean;
    merge(other: ValidationResult): void;
    mergeEnumValues(other: ValidationResult): void;
    mergeWarningGeneric(sub: ValidationResult, problemTypesToMerge: ProblemType[]): void;
    mergePropertyMatch(propertyValidationResult: ValidationResult, mergeEvaluated?: boolean): void;
    private mergeSources;
    compareGeneric(other: ValidationResult): number;
    compareKubernetes(other: ValidationResult): number;
}
export interface Options {
    isKubernetes: boolean;
    disableAdditionalProperties: boolean;
    uri: string;
    callFromAutoComplete?: boolean;
}
interface IValidationMatch {
    schema: JSONSchema;
    validationResult: ValidationResult;
    matchingSchemas: ISchemaCollector;
}
export declare abstract class BaseValidator {
    protected collectSeenKeys(node: ObjectASTNode): Record<string, ASTNode>;
    validateDocument(root: ASTNode, textDocument: TextDocument, schema: JSONSchema, options: Options): Diagnostic[];
    getMatchingSchemas(root: ASTNode, schema: JSONSchema, options: Options, focusOffset: number, exclude: ASTNode | null): IApplicableSchema[];
    protected getNoOpCollector(): ISchemaCollector;
    protected getSchemaSource(schema: JSONSchema, originalSchema: JSONSchema): string;
    protected getSchemaUri(schema: JSONSchema, originalSchema: JSONSchema): string[];
    /**
     * Draft-specific hook: interpret numeric bounds in the draft’s way.
     * - Draft07+: numeric exclusiveMinimum/exclusiveMaximum
     * - Draft04: boolean exclusiveMinimum/exclusiveMaximum that modify minimum/maximum
     */
    protected abstract getNumberLimits(schema: JSONSchema): {
        minimum?: number;
        maximum?: number;
        exclusiveMinimum?: number;
        exclusiveMaximum?: number;
    };
    /**
     * Get the current validator's dialect.
     */
    protected abstract getCurrentDialect(): SchemaDialect;
    protected validateNode(node: ASTNode, schema: JSONSchema, originalSchema: JSONSchema, validationResult: ValidationResult, matchingSchemas: ISchemaCollector, options: Options): void;
    protected validateGenericNode(node: ASTNode, schema: JSONSchema, originalSchema: JSONSchema, validationResult: ValidationResult, matchingSchemas: ISchemaCollector, options: Options): void;
    protected validateStringNode(node: StringASTNode, schema: JSONSchema, originalSchema: JSONSchema, validationResult: ValidationResult): void;
    protected validateNumberNode(node: NumberASTNode, schema: JSONSchema, originalSchema: JSONSchema, validationResult: ValidationResult): void;
    protected validateArrayNode(node: ArrayASTNode, schema: JSONSchema, originalSchema: JSONSchema, validationResult: ValidationResult, matchingSchemas: ISchemaCollector, options: Options): void;
    protected applyContains(node: ArrayASTNode, schema: JSONSchema, originalSchema: JSONSchema, validationResult: ValidationResult, _matchingSchemas: ISchemaCollector, options: Options): void;
    protected applyArrayLength(node: ArrayASTNode, schema: JSONSchema, originalSchema: JSONSchema, validationResult: ValidationResult, _options: Options): void;
    protected applyUniqueItems(node: ArrayASTNode, schema: JSONSchema, originalSchema: JSONSchema, validationResult: ValidationResult): void;
    protected validateObjectNode(node: ObjectASTNode, schema: JSONSchema, originalSchema: JSONSchema, validationResult: ValidationResult, matchingSchemas: ISchemaCollector, options: Options): void;
    protected applyRequired(node: ObjectASTNode, schema: JSONSchema, originalSchema: JSONSchema, validationResult: ValidationResult, _options: Options, seenKeys: Record<string, ASTNode>): void;
    protected applyProperties(_node: ObjectASTNode, schema: JSONSchema, originalSchema: JSONSchema, validationResult: ValidationResult, matchingSchemas: ISchemaCollector, options: Options, seenKeys: Record<string, ASTNode>, _unprocessedProperties: string[], propertyProcessed: (prop: string) => void): void;
    protected applyPatternProperties(_node: ObjectASTNode, schema: JSONSchema, originalSchema: JSONSchema, validationResult: ValidationResult, matchingSchemas: ISchemaCollector, options: Options, seenKeys: Record<string, ASTNode>, unprocessedProperties: string[], propertyProcessed: (prop: string) => void): void;
    protected applyAdditionalProperties(_node: ObjectASTNode, schema: JSONSchema, originalSchema: JSONSchema, validationResult: ValidationResult, matchingSchemas: ISchemaCollector, options: Options, seenKeys: Record<string, ASTNode>, unprocessedProperties: string[]): void;
    protected applyPropertyCount(node: ObjectASTNode, schema: JSONSchema, originalSchema: JSONSchema, validationResult: ValidationResult): void;
    protected applyDependencies(node: ObjectASTNode, schema: JSONSchema, originalSchema: JSONSchema, validationResult: ValidationResult, matchingSchemas: ISchemaCollector, options: Options, seenKeys: Record<string, ASTNode>): void;
    protected applyPropertyNames(node: ObjectASTNode, schema: JSONSchema, validationResult: ValidationResult, options: Options): void;
    protected applyUnevaluatedProperties(_node: ObjectASTNode, _schema: JSONSchema, _originalSchema: JSONSchema, _validationResult: ValidationResult, _matchingSchemas: ISchemaCollector, _options: Options, _seenKeys?: Record<string, ASTNode>, _unprocessedProperties?: string[]): void;
    protected applyUnevaluatedItems(_node: ArrayASTNode | ASTNode, _schema: JSONSchema, _originalSchema: JSONSchema, _validationResult: ValidationResult, _matchingSchemas: ISchemaCollector, _options: Options): void;
    protected alternativeComparison(subValidationResult: ValidationResult, bestMatch: IValidationMatch, subSchema: JSONSchema, subMatchingSchemas: ISchemaCollector): IValidationMatch;
    protected genericComparison(node: ASTNode, maxOneMatch: boolean, subValidationResult: ValidationResult, bestMatch: IValidationMatch, subSchema: JSONSchema, subMatchingSchemas: ISchemaCollector): IValidationMatch;
    protected mergeValidationMatches(bestMatch: IValidationMatch, subMatchingSchemas: ISchemaCollector, subValidationResult: ValidationResult): void;
}
export declare function asSchema(schema: JSONSchemaRef): JSONSchema | undefined;
export {};
