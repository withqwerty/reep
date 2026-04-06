/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Red Hat, Inc. All rights reserved.
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/
import { equals, isBoolean, isDefined, isIterable, isNumber, isString } from '../../utils/objects';
import { getSchemaTypeName } from '../../utils/schemaUtils';
import { isArrayEqual } from '../../utils/arrUtils';
import { safeCreateUnicodeRegExp } from '../../utils/strings';
import { FilePatternAssociation } from '../../utils/filePatternAssociation';
import { floatSafeRemainder } from '../../utils/math';
import { ErrorCode } from 'vscode-json-languageservice';
import * as l10n from '@vscode/l10n';
import { URI } from 'vscode-uri';
import { Diagnostic, DiagnosticSeverity, Range } from 'vscode-languageserver-types';
import { contains, getNodeValue } from '../astNodeUtils';
import { getValidator } from './validatorFactory';
export const YAML_SOURCE = 'YAML';
const YAML_SCHEMA_PREFIX = 'yaml-schema: ';
export var ProblemType;
(function (ProblemType) {
    ProblemType["missingRequiredPropWarning"] = "missingRequiredPropWarning";
    ProblemType["typeMismatchWarning"] = "typeMismatchWarning";
    ProblemType["constWarning"] = "constWarning";
})(ProblemType || (ProblemType = {}));
export const ProblemTypeMessages = {
    [ProblemType.missingRequiredPropWarning]: 'Missing property "{0}".',
    [ProblemType.typeMismatchWarning]: 'Incorrect type. Expected "{0}".',
    [ProblemType.constWarning]: 'Value must be {0}.',
};
class SchemaCollector {
    constructor(focusOffset = -1, exclude = null) {
        this.focusOffset = focusOffset;
        this.exclude = exclude;
        this.schemas = [];
    }
    add(schema) {
        this.schemas.push(schema);
    }
    merge(other) {
        this.schemas.push(...other.schemas);
    }
    include(node) {
        return (this.focusOffset === -1 || contains(node, this.focusOffset)) && node !== this.exclude;
    }
    newSub() {
        return new SchemaCollector(-1, this.exclude);
    }
}
class NoOpSchemaCollector {
    constructor() {
        // ignore
    }
    get schemas() {
        return [];
    }
    add() {
        // ignore
    }
    merge() {
        // ignore
    }
    include() {
        return true;
    }
    newSub() {
        return this;
    }
}
NoOpSchemaCollector.instance = new NoOpSchemaCollector();
export const formats = {
    'color-hex': {
        errorMessage: l10n.t('Invalid color format. Use #RGB, #RGBA, #RRGGBB or #RRGGBBAA.'),
        pattern: /^#([0-9A-Fa-f]{3,4}|([0-9A-Fa-f]{2}){3,4})$/,
    },
    'date-time': {
        errorMessage: l10n.t('String is not a RFC3339 date-time.'),
        pattern: /^(\d{4})-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])T([01][0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9]|60)(\.[0-9]+)?(Z|(\+|-)([01][0-9]|2[0-3]):([0-5][0-9]))$/i,
    },
    date: {
        errorMessage: l10n.t('String is not a RFC3339 date.'),
        pattern: /^(\d{4})-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$/i,
    },
    time: {
        errorMessage: l10n.t('String is not a RFC3339 time.'),
        pattern: /^([01][0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9]|60)(\.[0-9]+)?(Z|(\+|-)([01][0-9]|2[0-3]):([0-5][0-9]))$/i,
    },
    email: {
        errorMessage: l10n.t('String is not an e-mail address.'),
        pattern: /^(([^<>()[\]\\.,;:\s@"]+(\.[^<>()[\]\\.,;:\s@"]+)*)|(".+"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/,
    },
    ipv4: {
        errorMessage: l10n.t('String does not match IPv4 format.'),
        pattern: /^((25[0-5]|(2[0-4]|1\d|[1-9]|)\d)\.?\b){4}$/,
    },
    ipv6: {
        errorMessage: l10n.t('String does not match IPv6 format.'),
        pattern: /^([0-9a-f]|:){1,4}(:([0-9a-f]{0,4})*){1,7}$/i,
    },
};
export class ValidationResult {
    constructor(isKubernetes) {
        this.problems = [];
        this.propertiesMatches = 0;
        this.propertiesValueMatches = 0;
        this.primaryValueMatches = 0;
        this.enumValueMatch = false;
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        this.enumValues = null;
        if (isKubernetes)
            this.enumValues = [];
    }
    getEvaluatedItems(node) {
        this.evaluatedItemsByNode ?? (this.evaluatedItemsByNode = new Map());
        let evaluated = this.evaluatedItemsByNode.get(node);
        if (!evaluated) {
            evaluated = new Set();
            this.evaluatedItemsByNode.set(node, evaluated);
        }
        return evaluated;
    }
    hasProblems() {
        return this.problems.length > 0;
    }
    merge(other) {
        this.problems = this.problems.concat(other.problems);
        // Merge evaluatedProperties/evaluatedItems if present (needed for unevaluatedProperties/unevaluatedItems)
        if (other.evaluatedProperties) {
            this.evaluatedProperties ?? (this.evaluatedProperties = new Set());
            for (const p of other.evaluatedProperties)
                this.evaluatedProperties.add(p);
        }
        if (other.evaluatedItemsByNode) {
            this.evaluatedItemsByNode ?? (this.evaluatedItemsByNode = new Map());
            for (const [node, set] of other.evaluatedItemsByNode) {
                const target = this.evaluatedItemsByNode.get(node);
                if (target) {
                    for (const i of set)
                        target.add(i);
                }
                else {
                    this.evaluatedItemsByNode.set(node, new Set(set));
                }
            }
        }
    }
    mergeEnumValues(other) {
        if (!this.enumValueMatch && !other.enumValueMatch && this.enumValues && other.enumValues) {
            this.enumValues = this.enumValues.concat(other.enumValues);
            for (const err of this.problems) {
                if (err.code === ErrorCode.EnumValueMismatch) {
                    err.message = l10n.t('Value is not accepted. Valid values: {0}.', [...new Set(this.enumValues)].map((v) => JSON.stringify(v)).join(', '));
                }
            }
        }
    }
    mergeWarningGeneric(sub, problemTypesToMerge) {
        if (!this.problems?.length)
            return;
        for (const problemType of problemTypesToMerge) {
            const bestResults = this.problems.filter((p) => p.problemType === problemType);
            for (const bestResult of bestResults) {
                const mergingResult = sub.problems?.find((p) => p.problemType === problemType &&
                    bestResult.location.offset === p.location.offset &&
                    (problemType !== ProblemType.missingRequiredPropWarning || isArrayEqual(p.problemArgs, bestResult.problemArgs)));
                if (!mergingResult)
                    continue;
                if (mergingResult.problemArgs?.length) {
                    mergingResult.problemArgs
                        .filter((p) => !bestResult.problemArgs.includes(p))
                        .forEach((p) => bestResult.problemArgs.push(p));
                    if (bestResult.problemType) {
                        bestResult.message = getWarningMessage(bestResult.problemType, bestResult.problemArgs ?? []);
                    }
                }
                this.mergeSources(mergingResult, bestResult);
            }
        }
    }
    mergePropertyMatch(propertyValidationResult, mergeEvaluated = true) {
        if (mergeEvaluated) {
            this.merge(propertyValidationResult);
        }
        else {
            this.problems = this.problems.concat(propertyValidationResult.problems);
        }
        this.propertiesMatches++;
        if (propertyValidationResult.enumValueMatch ||
            (!propertyValidationResult.hasProblems() && propertyValidationResult.propertiesMatches)) {
            this.propertiesValueMatches++;
        }
        if (propertyValidationResult.enumValueMatch && propertyValidationResult.enumValues) {
            this.primaryValueMatches++;
        }
    }
    mergeSources(mergingResult, bestResult) {
        const mergingSource = (mergingResult.source ?? '').replace(YAML_SCHEMA_PREFIX, '');
        if (mergingSource && bestResult.source && !bestResult.source.includes(mergingSource)) {
            bestResult.source = bestResult.source + ' | ' + mergingSource;
        }
        if (bestResult.schemaUri && mergingResult.schemaUri && !bestResult.schemaUri.includes(mergingResult.schemaUri[0])) {
            bestResult.schemaUri = bestResult.schemaUri.concat(mergingResult.schemaUri);
        }
    }
    compareGeneric(other) {
        const hasProblems = this.hasProblems();
        if (hasProblems !== other.hasProblems())
            return hasProblems ? -1 : 1;
        if (this.enumValueMatch !== other.enumValueMatch)
            return other.enumValueMatch ? -1 : 1;
        if (this.propertiesValueMatches !== other.propertiesValueMatches)
            return this.propertiesValueMatches - other.propertiesValueMatches;
        if (this.primaryValueMatches !== other.primaryValueMatches)
            return this.primaryValueMatches - other.primaryValueMatches;
        return this.propertiesMatches - other.propertiesMatches;
    }
    compareKubernetes(other) {
        const hasProblems = this.hasProblems();
        if (this.propertiesMatches !== other.propertiesMatches)
            return this.propertiesMatches - other.propertiesMatches;
        if (this.enumValueMatch !== other.enumValueMatch)
            return other.enumValueMatch ? -1 : 1;
        if (this.primaryValueMatches !== other.primaryValueMatches)
            return this.primaryValueMatches - other.primaryValueMatches;
        if (this.propertiesValueMatches !== other.propertiesValueMatches)
            return this.propertiesValueMatches - other.propertiesValueMatches;
        if (hasProblems !== other.hasProblems())
            return hasProblems ? -1 : 1;
        return this.propertiesMatches - other.propertiesMatches;
    }
}
export class BaseValidator {
    collectSeenKeys(node) {
        const seenKeys = Object.create(null);
        const unprocessedNodes = [...node.properties];
        while (unprocessedNodes.length > 0) {
            const propertyNode = unprocessedNodes.pop();
            if (!propertyNode)
                continue;
            const key = propertyNode.keyNode.value;
            // YAML merge key "<<"
            if (key === '<<' && propertyNode.valueNode) {
                const valueNode = propertyNode.valueNode;
                switch (valueNode.type) {
                    case 'object':
                        unprocessedNodes.push(...valueNode.properties);
                        break;
                    case 'array':
                        valueNode.items.forEach((sequenceNode) => {
                            if (sequenceNode && sequenceNode.type === 'object' && isIterable(sequenceNode.properties)) {
                                unprocessedNodes.push(...sequenceNode.properties);
                            }
                        });
                        break;
                    default:
                        break;
                }
            }
            else {
                seenKeys[key] = propertyNode.valueNode;
            }
        }
        return seenKeys;
    }
    validateDocument(root, textDocument, schema, options) {
        const validationResult = new ValidationResult(options.isKubernetes);
        this.validateNode(root, schema, schema, validationResult, NoOpSchemaCollector.instance, options);
        return validationResult.problems.map((p) => {
            const range = Range.create(textDocument.positionAt(p.location.offset), textDocument.positionAt(p.location.offset + p.location.length));
            const diagnostic = Diagnostic.create(range, p.message, p.severity, p.code ? p.code : ErrorCode.Undefined, p.source);
            diagnostic.data = { schemaUri: p.schemaUri, ...p.data };
            return diagnostic;
        });
    }
    getMatchingSchemas(root, schema, options, focusOffset, exclude) {
        const matchingSchemas = new SchemaCollector(focusOffset, exclude);
        this.validateNode(root, schema, schema, new ValidationResult(options.isKubernetes), matchingSchemas, options);
        return matchingSchemas.schemas;
    }
    getNoOpCollector() {
        return NoOpSchemaCollector.instance;
    }
    getSchemaSource(schema, originalSchema) {
        let label;
        if (schema.title) {
            label = schema.title;
        }
        else if (schema.closestTitle) {
            label = schema.closestTitle;
        }
        else if (originalSchema.closestTitle) {
            label = originalSchema.closestTitle;
        }
        else {
            const uriString = schema.url ?? originalSchema.url;
            if (uriString)
                label = URI.parse(uriString).toString();
        }
        return label ? `${YAML_SCHEMA_PREFIX}${label}` : YAML_SOURCE;
    }
    getSchemaUri(schema, originalSchema) {
        const uriString = schema.url ?? originalSchema.url;
        return uriString ? [uriString] : [];
    }
    // ---------------- core traversal ----------------
    validateNode(node, schema, originalSchema, validationResult, matchingSchemas, options) {
        if (!node)
            return;
        if (!schema || typeof schema !== 'object')
            return;
        // Draft 2020-12 Compound Schema Document behavior: check if this node explicitly declares a different dialect
        if (schema._dialect) {
            const subDialect = schema._dialect;
            const currentDialect = this.getCurrentDialect();
            if (subDialect !== currentDialect) {
                const subValidator = getValidator(subDialect);
                subValidator.validateNode(node, schema, originalSchema, validationResult, matchingSchemas, options);
                return;
            }
        }
        if (!schema.url)
            schema.url = originalSchema.url;
        schema.closestTitle = schema.title || originalSchema.closestTitle;
        switch (node.type) {
            case 'object':
                this.validateObjectNode(node, schema, originalSchema, validationResult, matchingSchemas, options);
                break;
            case 'array':
                this.validateArrayNode(node, schema, originalSchema, validationResult, matchingSchemas, options);
                break;
            case 'string':
                this.validateStringNode(node, schema, originalSchema, validationResult);
                break;
            case 'number':
                this.validateNumberNode(node, schema, originalSchema, validationResult);
                break;
            case 'property':
                this.validateNode(node.valueNode, schema, schema, validationResult, matchingSchemas, options);
                break;
            default:
                break;
        }
        this.validateGenericNode(node, schema, originalSchema, validationResult, matchingSchemas, options);
        // used by completion/hover schema collection
        matchingSchemas.add({ node, schema });
        // draft-specific post-processing hooks (no-op in base)
        this.applyUnevaluatedItems(node, schema, originalSchema, validationResult, matchingSchemas, options);
        // unevaluatedProperties must run AFTER validateGenericNode() (allOf/$ref/anyOf/oneOf/if/then/else/not)
        if (node.type === 'object') {
            const seenKeys = this.collectSeenKeys(node);
            this.applyUnevaluatedProperties(node, schema, originalSchema, validationResult, matchingSchemas, options, seenKeys);
        }
    }
    // ---------------- shared generic keywords ----------------
    validateGenericNode(node, schema, originalSchema, validationResult, matchingSchemas, options) {
        const { isKubernetes, callFromAutoComplete } = options;
        const matchesType = (type) => {
            return node.type === type || (type === 'integer' && node.type === 'number' && node.isInteger);
        };
        const mergeEvaluated = (target, source) => {
            if (source.evaluatedProperties) {
                target.evaluatedProperties ?? (target.evaluatedProperties = new Set());
                for (const p of source.evaluatedProperties)
                    target.evaluatedProperties.add(p);
            }
            if (source.evaluatedItemsByNode) {
                target.evaluatedItemsByNode ?? (target.evaluatedItemsByNode = new Map());
                for (const [nodeRef, set] of source.evaluatedItemsByNode) {
                    const targetSet = target.evaluatedItemsByNode.get(nodeRef);
                    if (targetSet) {
                        for (const i of set)
                            targetSet.add(i);
                    }
                    else {
                        target.evaluatedItemsByNode.set(nodeRef, new Set(set));
                    }
                }
            }
        };
        // type
        if (Array.isArray(schema.type)) {
            if (!schema.type.some(matchesType)) {
                validationResult.problems.push({
                    location: { offset: node.offset, length: node.length },
                    severity: DiagnosticSeverity.Warning,
                    message: schema.errorMessage || l10n.t('Incorrect type. Expected one of {0}.', schema.type.join(', ')),
                    source: this.getSchemaSource(schema, originalSchema),
                    schemaUri: this.getSchemaUri(schema, originalSchema),
                });
            }
        }
        else if (schema.type) {
            if (!matchesType(schema.type)) {
                const schemaType = schema.type === 'object' ? getSchemaTypeName(schema) : schema.type;
                validationResult.problems.push({
                    location: { offset: node.offset, length: node.length },
                    severity: DiagnosticSeverity.Warning,
                    message: schema.errorMessage || getWarningMessage(ProblemType.typeMismatchWarning, [schemaType]),
                    source: this.getSchemaSource(schema, originalSchema),
                    schemaUri: this.getSchemaUri(schema, originalSchema),
                    problemType: ProblemType.typeMismatchWarning,
                    problemArgs: [schemaType],
                });
            }
        }
        // allOf
        if (Array.isArray(schema.allOf)) {
            for (const subSchemaRef of schema.allOf) {
                const subSchema = asSchema(subSchemaRef);
                const subValidationResult = new ValidationResult(isKubernetes);
                const subMatchingSchemas = matchingSchemas.newSub();
                this.validateNode(node, subSchema, schema, subValidationResult, subMatchingSchemas, options);
                validationResult.merge(subValidationResult);
                validationResult.propertiesMatches += subValidationResult.propertiesMatches;
                validationResult.propertiesValueMatches += subValidationResult.propertiesValueMatches;
                matchingSchemas.merge(subMatchingSchemas);
            }
        }
        // not
        const notSchema = asSchema(schema.not);
        if (notSchema) {
            const subValidationResult = new ValidationResult(isKubernetes);
            const subMatchingSchemas = matchingSchemas.newSub();
            this.validateNode(node, notSchema, schema, subValidationResult, subMatchingSchemas, options);
            if (!subValidationResult.hasProblems()) {
                validationResult.problems.push({
                    location: { offset: node.offset, length: node.length },
                    severity: DiagnosticSeverity.Warning,
                    message: l10n.t('Matches a schema that is not allowed.'),
                    source: this.getSchemaSource(schema, originalSchema),
                    schemaUri: this.getSchemaUri(schema, originalSchema),
                });
            }
            for (const ms of subMatchingSchemas.schemas) {
                ms.inverted = !ms.inverted;
                matchingSchemas.add(ms);
            }
        }
        const testAlternatives = (alternatives, maxOneMatch) => {
            const subMatches = [];
            const noPropertyMatches = [];
            const validResults = [];
            let bestMatch = null;
            for (const subSchemaRef of alternatives) {
                const subSchema = { ...asSchema(subSchemaRef) };
                const subValidationResult = new ValidationResult(isKubernetes);
                const subMatchingSchemas = matchingSchemas.newSub();
                this.validateNode(node, subSchema, schema, subValidationResult, subMatchingSchemas, options);
                if (!subValidationResult.hasProblems() || callFromAutoComplete) {
                    subMatches.push(subSchema);
                    if (subValidationResult.propertiesMatches === 0)
                        noPropertyMatches.push(subSchema);
                    if (subSchema.format)
                        subMatches.pop();
                }
                if (!subValidationResult.hasProblems()) {
                    validResults.push(subValidationResult);
                }
                if (!bestMatch) {
                    bestMatch = { schema: subSchema, validationResult: subValidationResult, matchingSchemas: subMatchingSchemas };
                }
                else if (isKubernetes) {
                    bestMatch = this.alternativeComparison(subValidationResult, bestMatch, subSchema, subMatchingSchemas);
                }
                else {
                    bestMatch = this.genericComparison(node, maxOneMatch, subValidationResult, bestMatch, subSchema, subMatchingSchemas);
                }
            }
            if (subMatches.length > 1 && (subMatches.length > 1 || noPropertyMatches.length === 0) && maxOneMatch) {
                validationResult.problems.push({
                    location: { offset: node.offset, length: 1 },
                    severity: DiagnosticSeverity.Warning,
                    message: l10n.t('Matches multiple schemas when only one must validate.'),
                    source: this.getSchemaSource(schema, originalSchema),
                    schemaUri: this.getSchemaUri(schema, originalSchema),
                });
            }
            if (bestMatch) {
                validationResult.merge(bestMatch.validationResult);
                validationResult.propertiesMatches += bestMatch.validationResult.propertiesMatches;
                validationResult.propertiesValueMatches += bestMatch.validationResult.propertiesValueMatches;
                validationResult.enumValueMatch = validationResult.enumValueMatch || bestMatch.validationResult.enumValueMatch;
                if (bestMatch.validationResult.enumValues?.length) {
                    validationResult.enumValues = (validationResult.enumValues || []).concat(bestMatch.validationResult.enumValues);
                }
                matchingSchemas.merge(bestMatch.matchingSchemas);
            }
            if (validResults.length > 0) {
                for (const result of validResults) {
                    mergeEvaluated(validationResult, result);
                }
            }
        };
        if (Array.isArray(schema.anyOf)) {
            testAlternatives(schema.anyOf, false);
        }
        if (Array.isArray(schema.oneOf)) {
            testAlternatives(schema.oneOf, true);
        }
        // if / then / else (plus filePatternAssociation extension)
        const testBranch = (branchSchema, original) => {
            const subValidationResult = new ValidationResult(isKubernetes);
            const subMatchingSchemas = matchingSchemas.newSub();
            this.validateNode(node, asSchema(branchSchema), original, subValidationResult, subMatchingSchemas, options);
            validationResult.merge(subValidationResult);
            validationResult.propertiesMatches += subValidationResult.propertiesMatches;
            validationResult.propertiesValueMatches += subValidationResult.propertiesValueMatches;
            matchingSchemas.merge(subMatchingSchemas);
        };
        const testCondition = (ifSchemaRef, original, thenSchema, elseSchema) => {
            const subSchema = asSchema(ifSchemaRef);
            const subValidationResult = new ValidationResult(isKubernetes);
            const subMatchingSchemas = matchingSchemas.newSub();
            this.validateNode(node, subSchema, original, subValidationResult, subMatchingSchemas, options);
            matchingSchemas.merge(subMatchingSchemas);
            const filePatternAssociation = subSchema?.filePatternAssociation;
            if (filePatternAssociation) {
                const association = new FilePatternAssociation(filePatternAssociation);
                if (!association.matchesPattern(options.uri)) {
                    subValidationResult.problems.push({
                        location: { offset: node.offset, length: node.length },
                        severity: DiagnosticSeverity.Warning,
                        message: l10n.t("filePatternAssociation '{0}' does not match with doc uri '{1}'", filePatternAssociation, options.uri),
                        source: this.getSchemaSource(schema, originalSchema),
                        schemaUri: this.getSchemaUri(schema, originalSchema),
                    });
                }
            }
            if (!subValidationResult.hasProblems()) {
                mergeEvaluated(validationResult, subValidationResult);
                if (thenSchema)
                    testBranch(thenSchema, original);
            }
            else if (elseSchema) {
                testBranch(elseSchema, original);
            }
        };
        const ifSchema = asSchema(schema.if);
        if (ifSchema) {
            testCondition(ifSchema, schema, asSchema(schema.then), asSchema(schema.else));
        }
        // enum
        if (Array.isArray(schema.enum)) {
            const val = getNodeValue(node);
            let enumValueMatch = false;
            for (const e of schema.enum) {
                if (equals(val, e, node.type) || isAutoCompleteEqualMaybe(callFromAutoComplete, node, val, e)) {
                    enumValueMatch = true;
                    break;
                }
            }
            validationResult.enumValues = schema.enum;
            validationResult.enumValueMatch = enumValueMatch;
            if (!enumValueMatch) {
                validationResult.problems.push({
                    location: { offset: node.offset, length: node.length },
                    severity: DiagnosticSeverity.Warning,
                    code: ErrorCode.EnumValueMismatch,
                    message: schema.errorMessage ||
                        l10n.t('Value is not accepted. Valid values: {0}.', (schema.enum ?? []).map((v) => JSON.stringify(v)).join(', ')),
                    source: this.getSchemaSource(schema, originalSchema),
                    schemaUri: this.getSchemaUri(schema, originalSchema),
                    data: { values: schema.enum },
                });
            }
        }
        // const
        if (isDefined(schema.const)) {
            const val = getNodeValue(node);
            const c = schema.const;
            if (!equals(val, c, node.type) && !isAutoCompleteEqualMaybe(callFromAutoComplete, node, val, c)) {
                validationResult.problems.push({
                    location: { offset: node.offset, length: node.length },
                    severity: DiagnosticSeverity.Warning,
                    code: ErrorCode.EnumValueMismatch,
                    problemType: ProblemType.constWarning,
                    message: schema.errorMessage || getWarningMessage(ProblemType.constWarning, [JSON.stringify(c)]),
                    source: this.getSchemaSource(schema, originalSchema),
                    schemaUri: this.getSchemaUri(schema, originalSchema),
                    problemArgs: [JSON.stringify(c)],
                    data: { values: [c] },
                });
                validationResult.enumValueMatch = false;
            }
            else {
                validationResult.enumValueMatch = true;
            }
            validationResult.enumValues = [c];
        }
        // deprecationMessage
        if (schema.deprecationMessage && node.parent) {
            validationResult.problems.push({
                location: { offset: node.parent.offset, length: node.parent.length },
                severity: DiagnosticSeverity.Warning,
                message: schema.deprecationMessage,
                source: this.getSchemaSource(schema, originalSchema),
                schemaUri: this.getSchemaUri(schema, originalSchema),
            });
        }
    }
    // ---------------- shared leaf validations ----------------
    validateStringNode(node, schema, originalSchema, validationResult) {
        const value = node.value;
        if (isNumber(schema.minLength) && value.length < (schema.minLength ?? 0)) {
            validationResult.problems.push({
                location: { offset: node.offset, length: node.length },
                severity: DiagnosticSeverity.Warning,
                message: l10n.t('String is shorter than the minimum length of {0}.', schema.minLength),
                source: this.getSchemaSource(schema, originalSchema),
                schemaUri: this.getSchemaUri(schema, originalSchema),
            });
        }
        if (isNumber(schema.maxLength) && value.length > (schema.maxLength ?? 0)) {
            validationResult.problems.push({
                location: { offset: node.offset, length: node.length },
                severity: DiagnosticSeverity.Warning,
                message: l10n.t('String is longer than the maximum length of {0}.', schema.maxLength),
                source: this.getSchemaSource(schema, originalSchema),
                schemaUri: this.getSchemaUri(schema, originalSchema),
            });
        }
        if (isString(schema.pattern)) {
            const regex = safeCreateUnicodeRegExp(schema.pattern);
            if (!regex.test(value)) {
                validationResult.problems.push({
                    location: { offset: node.offset, length: node.length },
                    severity: DiagnosticSeverity.Warning,
                    message: schema.patternErrorMessage ||
                        schema.errorMessage ||
                        l10n.t('String does not match the pattern of "{0}".', schema.pattern),
                    source: this.getSchemaSource(schema, originalSchema),
                    schemaUri: this.getSchemaUri(schema, originalSchema),
                });
            }
        }
        if (schema.format) {
            switch (schema.format) {
                case 'uri':
                case 'uri-reference': {
                    let errorMessage;
                    if (!value) {
                        errorMessage = l10n.t('URI expected.');
                    }
                    else {
                        try {
                            const uri = URI.parse(value);
                            if (!uri.scheme && schema.format === 'uri') {
                                errorMessage = l10n.t('URI with a scheme is expected.');
                            }
                        }
                        catch (e) {
                            errorMessage = e instanceof Error ? e.message : String(e);
                        }
                    }
                    if (errorMessage) {
                        validationResult.problems.push({
                            location: { offset: node.offset, length: node.length },
                            severity: DiagnosticSeverity.Warning,
                            message: schema.patternErrorMessage || schema.errorMessage || l10n.t('String is not a URI: {0}', errorMessage),
                            source: this.getSchemaSource(schema, originalSchema),
                            schemaUri: this.getSchemaUri(schema, originalSchema),
                        });
                    }
                    break;
                }
                case 'color-hex':
                case 'date-time':
                case 'date':
                case 'time':
                case 'email':
                case 'ipv4':
                case 'ipv6': {
                    const format = formats[schema.format];
                    if (!value || !format.pattern.test(value)) {
                        validationResult.problems.push({
                            location: { offset: node.offset, length: node.length },
                            severity: DiagnosticSeverity.Warning,
                            message: schema.patternErrorMessage || schema.errorMessage || l10n.t(format.errorMessage),
                            source: this.getSchemaSource(schema, originalSchema),
                            schemaUri: this.getSchemaUri(schema, originalSchema),
                        });
                    }
                    break;
                }
                default:
                    break;
            }
        }
    }
    validateNumberNode(node, schema, originalSchema, validationResult) {
        const val = node.value;
        if (isNumber(schema.multipleOf)) {
            if (floatSafeRemainder(val, schema.multipleOf ?? 0) !== 0) {
                validationResult.problems.push({
                    location: { offset: node.offset, length: node.length },
                    severity: DiagnosticSeverity.Warning,
                    message: l10n.t('Value is not divisible by {0}.', schema.multipleOf),
                    source: this.getSchemaSource(schema, originalSchema),
                    schemaUri: this.getSchemaUri(schema, originalSchema),
                });
            }
        }
        const limits = this.getNumberLimits(schema);
        if (isNumber(limits.exclusiveMinimum) && val <= limits.exclusiveMinimum) {
            validationResult.problems.push({
                location: { offset: node.offset, length: node.length },
                severity: DiagnosticSeverity.Warning,
                message: l10n.t('Value is below the exclusive minimum of {0}.', limits.exclusiveMinimum),
                source: this.getSchemaSource(schema, originalSchema),
                schemaUri: this.getSchemaUri(schema, originalSchema),
            });
        }
        if (isNumber(limits.exclusiveMaximum) && val >= limits.exclusiveMaximum) {
            validationResult.problems.push({
                location: { offset: node.offset, length: node.length },
                severity: DiagnosticSeverity.Warning,
                message: l10n.t('Value is above the exclusive maximum of {0}.', limits.exclusiveMaximum),
                source: this.getSchemaSource(schema, originalSchema),
                schemaUri: this.getSchemaUri(schema, originalSchema),
            });
        }
        if (isNumber(limits.minimum) && val < limits.minimum) {
            validationResult.problems.push({
                location: { offset: node.offset, length: node.length },
                severity: DiagnosticSeverity.Warning,
                message: l10n.t('Value is below the minimum of {0}.', limits.minimum),
                source: this.getSchemaSource(schema, originalSchema),
                schemaUri: this.getSchemaUri(schema, originalSchema),
            });
        }
        if (isNumber(limits.maximum) && val > limits.maximum) {
            validationResult.problems.push({
                location: { offset: node.offset, length: node.length },
                severity: DiagnosticSeverity.Warning,
                message: l10n.t('Value is above the maximum of {0}.', limits.maximum),
                source: this.getSchemaSource(schema, originalSchema),
                schemaUri: this.getSchemaUri(schema, originalSchema),
            });
        }
    }
    // ---------------- array validation (draft hotspot) ----------------
    validateArrayNode(node, schema, originalSchema, validationResult, matchingSchemas, options) {
        // Draft-07 default: tuple arrays via items: [] and additionalItems
        const { isKubernetes } = options;
        const items = node.items ?? [];
        const evaluatedItems = schema.items ? validationResult.getEvaluatedItems(node) : undefined;
        if (Array.isArray(schema.items)) {
            const subSchemas = schema.items;
            for (let index = 0; index < subSchemas.length; index++) {
                const subSchema = asSchema(subSchemas[index]);
                const itemValidationResult = new ValidationResult(isKubernetes);
                const item = items[index];
                if (item) {
                    this.validateNode(item, subSchema, schema, itemValidationResult, matchingSchemas, options);
                    validationResult.mergePropertyMatch(itemValidationResult, false);
                    validationResult.mergeEnumValues(itemValidationResult);
                    evaluatedItems?.add(index);
                }
                else if (items.length >= subSchemas.length) {
                    validationResult.propertiesValueMatches++;
                }
            }
            if (items.length > subSchemas.length) {
                const additional = schema.additionalItems;
                if (additional === false) {
                    validationResult.problems.push({
                        location: { offset: node.offset, length: node.length },
                        severity: DiagnosticSeverity.Warning,
                        message: l10n.t('Array has too many items according to schema. Expected {0} or fewer.', subSchemas.length),
                        source: this.getSchemaSource(schema, originalSchema),
                        schemaUri: this.getSchemaUri(schema, originalSchema),
                    });
                    for (let i = subSchemas.length; i < items.length; i++) {
                        evaluatedItems?.add(i);
                    }
                }
                else if (typeof additional === 'object') {
                    for (let i = subSchemas.length; i < items.length; i++) {
                        const itemValidationResult = new ValidationResult(isKubernetes);
                        this.validateNode(items[i], additional, schema, itemValidationResult, matchingSchemas, options);
                        validationResult.mergePropertyMatch(itemValidationResult, false);
                        validationResult.mergeEnumValues(itemValidationResult);
                        evaluatedItems?.add(i);
                    }
                }
                else if (additional === true) {
                    // additionalItems is true => allowed by default (treat as {}), so mark remaining as evaluated
                    for (let i = subSchemas.length; i < items.length; i++) {
                        evaluatedItems?.add(i);
                    }
                }
            }
        }
        else {
            const itemSchema = asSchema(schema.items);
            if (itemSchema) {
                const currentEvaluatedItems = validationResult.getEvaluatedItems(node);
                for (let i = 0; i < items.length; i++) {
                    const item = items[i];
                    const itemValidationResult = new ValidationResult(isKubernetes);
                    this.validateNode(item, itemSchema, schema, itemValidationResult, matchingSchemas, options);
                    validationResult.mergePropertyMatch(itemValidationResult, false);
                    validationResult.mergeEnumValues(itemValidationResult);
                    currentEvaluatedItems.add(i);
                }
            }
        }
        this.applyContains(node, schema, originalSchema, validationResult, matchingSchemas, options);
        this.applyArrayLength(node, schema, originalSchema, validationResult, options);
        this.applyUniqueItems(node, schema, originalSchema, validationResult);
    }
    applyContains(node, schema, originalSchema, validationResult, _matchingSchemas, options) {
        // Draft-07 default: contains must match at least one item
        const { isKubernetes } = options;
        const containsSchema = asSchema(schema.contains);
        if (!containsSchema)
            return;
        const doesContain = (node.items ?? []).some((item) => {
            const itemValidationResult = new ValidationResult(isKubernetes);
            this.validateNode(item, containsSchema, schema, itemValidationResult, NoOpSchemaCollector.instance, options);
            return !itemValidationResult.hasProblems();
        });
        if (!doesContain) {
            validationResult.problems.push({
                location: { offset: node.offset, length: node.length },
                severity: DiagnosticSeverity.Warning,
                message: schema.errorMessage || l10n.t('Array does not contain required item.'),
                source: this.getSchemaSource(schema, originalSchema),
                schemaUri: this.getSchemaUri(schema, originalSchema),
            });
        }
    }
    applyArrayLength(node, schema, originalSchema, validationResult, 
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    _options) {
        if (isNumber(schema.minItems) && node.items.length < (schema.minItems ?? 0)) {
            validationResult.problems.push({
                location: { offset: node.offset, length: node.length },
                severity: DiagnosticSeverity.Warning,
                message: l10n.t('Array has too few items. Expected {0} or more.', schema.minItems),
                source: this.getSchemaSource(schema, originalSchema),
                schemaUri: this.getSchemaUri(schema, originalSchema),
            });
        }
        if (isNumber(schema.maxItems) && node.items.length > (schema.maxItems ?? 0)) {
            validationResult.problems.push({
                location: { offset: node.offset, length: node.length },
                severity: DiagnosticSeverity.Warning,
                message: l10n.t('Array has too many items. Expected {0} or fewer.', schema.maxItems),
                source: this.getSchemaSource(schema, originalSchema),
                schemaUri: this.getSchemaUri(schema, originalSchema),
            });
        }
    }
    applyUniqueItems(node, schema, originalSchema, validationResult) {
        if (schema.uniqueItems === true) {
            const values = getNodeValue(node);
            const duplicates = Array.isArray(values) && values.some((v, i) => i !== values.lastIndexOf(v));
            if (duplicates) {
                validationResult.problems.push({
                    location: { offset: node.offset, length: node.length },
                    severity: DiagnosticSeverity.Warning,
                    message: l10n.t('Array has duplicate items.'),
                    source: this.getSchemaSource(schema, originalSchema),
                    schemaUri: this.getSchemaUri(schema, originalSchema),
                });
            }
        }
    }
    // ---------------- object validation ----------------
    validateObjectNode(node, schema, originalSchema, validationResult, matchingSchemas, options) {
        const seenKeys = Object.create(null);
        const unprocessedProperties = [];
        const unprocessedNodes = [...node.properties];
        while (unprocessedNodes.length > 0) {
            const propertyNode = unprocessedNodes.pop();
            if (!propertyNode)
                continue;
            const key = propertyNode.keyNode.value;
            // YAML merge key "<<"
            if (key === '<<' && propertyNode.valueNode) {
                switch (propertyNode.valueNode.type) {
                    case 'object':
                        unprocessedNodes.push(...propertyNode.valueNode.properties);
                        break;
                    case 'array':
                        propertyNode.valueNode.items.forEach((sequenceNode) => {
                            if (sequenceNode && sequenceNode.type === 'object' && isIterable(sequenceNode.properties)) {
                                unprocessedNodes.push(...sequenceNode.properties);
                            }
                        });
                        break;
                    default:
                        break;
                }
            }
            else {
                seenKeys[key] = propertyNode.valueNode;
                unprocessedProperties.push(key);
            }
        }
        const propertyProcessed = (prop) => {
            let index = unprocessedProperties.indexOf(prop);
            while (index >= 0) {
                unprocessedProperties.splice(index, 1);
                index = unprocessedProperties.indexOf(prop);
            }
        };
        this.applyRequired(node, schema, originalSchema, validationResult, options, seenKeys);
        this.applyProperties(node, schema, originalSchema, validationResult, matchingSchemas, options, seenKeys, unprocessedProperties, propertyProcessed);
        this.applyPatternProperties(node, schema, originalSchema, validationResult, matchingSchemas, options, seenKeys, unprocessedProperties, propertyProcessed);
        this.applyAdditionalProperties(node, schema, originalSchema, validationResult, matchingSchemas, options, seenKeys, unprocessedProperties);
        this.applyPropertyCount(node, schema, originalSchema, validationResult);
        this.applyDependencies(node, schema, originalSchema, validationResult, matchingSchemas, options, seenKeys);
        this.applyPropertyNames(node, schema, validationResult, options);
    }
    applyRequired(node, schema, originalSchema, validationResult, _options, seenKeys) {
        if (Array.isArray(schema.required)) {
            for (const propertyName of schema.required) {
                if (seenKeys[propertyName] === undefined) {
                    const keyNode = node.parent && node.parent.type === 'property' && node.parent.keyNode;
                    const location = keyNode ? { offset: keyNode.offset, length: keyNode.length } : { offset: node.offset, length: 1 };
                    validationResult.problems.push({
                        location,
                        severity: DiagnosticSeverity.Warning,
                        message: schema.errorMessage || getWarningMessage(ProblemType.missingRequiredPropWarning, [propertyName]),
                        source: this.getSchemaSource(schema, originalSchema),
                        schemaUri: this.getSchemaUri(schema, originalSchema),
                        problemArgs: [propertyName],
                        problemType: ProblemType.missingRequiredPropWarning,
                    });
                }
            }
        }
    }
    applyProperties(_node, schema, originalSchema, validationResult, matchingSchemas, options, seenKeys, _unprocessedProperties, propertyProcessed) {
        const { isKubernetes } = options;
        const props = schema.properties;
        if (!props)
            return;
        for (const propertyName of Object.keys(props)) {
            propertyProcessed(propertyName);
            const propertySchemaRef = props[propertyName];
            const child = seenKeys[propertyName];
            if (!child)
                continue;
            validationResult.evaluatedProperties ?? (validationResult.evaluatedProperties = new Set());
            validationResult.evaluatedProperties.add(propertyName);
            if (isBoolean(propertySchemaRef)) {
                if (!propertySchemaRef) {
                    const propertyNode = child.parent;
                    validationResult.problems.push({
                        location: { offset: propertyNode.keyNode.offset, length: propertyNode.keyNode.length },
                        severity: DiagnosticSeverity.Warning,
                        message: schema.errorMessage || l10n.t('Property {0} is not allowed.', propertyName),
                        source: this.getSchemaSource(schema, originalSchema),
                        schemaUri: this.getSchemaUri(schema, originalSchema),
                    });
                }
                else {
                    validationResult.propertiesMatches++;
                    validationResult.propertiesValueMatches++;
                }
            }
            else {
                propertySchemaRef.url = schema.url ?? originalSchema.url;
                const propertyValidationResult = new ValidationResult(isKubernetes);
                this.validateNode(child, propertySchemaRef, schema, propertyValidationResult, matchingSchemas, options);
                validationResult.mergePropertyMatch(propertyValidationResult, false);
                validationResult.mergeEnumValues(propertyValidationResult);
            }
        }
    }
    applyPatternProperties(_node, schema, originalSchema, validationResult, matchingSchemas, options, seenKeys, unprocessedProperties, propertyProcessed) {
        const { isKubernetes } = options;
        const patternProps = schema.patternProperties;
        if (!patternProps)
            return;
        for (const propertyPattern of Object.keys(patternProps)) {
            const regex = safeCreateUnicodeRegExp(propertyPattern);
            for (const propertyName of unprocessedProperties.slice(0)) {
                if (!regex.test(propertyName))
                    continue;
                propertyProcessed(propertyName);
                const child = seenKeys[propertyName];
                if (!child)
                    continue;
                validationResult.evaluatedProperties ?? (validationResult.evaluatedProperties = new Set());
                validationResult.evaluatedProperties.add(propertyName);
                const propertySchemaRef = patternProps[propertyPattern];
                if (isBoolean(propertySchemaRef)) {
                    if (!propertySchemaRef) {
                        const propertyNode = child.parent;
                        validationResult.problems.push({
                            location: { offset: propertyNode.keyNode.offset, length: propertyNode.keyNode.length },
                            severity: DiagnosticSeverity.Warning,
                            message: schema.errorMessage || l10n.t('Property {0} is not allowed.', propertyName),
                            source: this.getSchemaSource(schema, originalSchema),
                            schemaUri: this.getSchemaUri(schema, originalSchema),
                        });
                    }
                    else {
                        validationResult.propertiesMatches++;
                        validationResult.propertiesValueMatches++;
                    }
                }
                else {
                    const propertyValidationResult = new ValidationResult(isKubernetes);
                    this.validateNode(child, propertySchemaRef, schema, propertyValidationResult, matchingSchemas, options);
                    validationResult.mergePropertyMatch(propertyValidationResult, false);
                    validationResult.mergeEnumValues(propertyValidationResult);
                }
            }
        }
    }
    applyAdditionalProperties(_node, schema, originalSchema, validationResult, matchingSchemas, options, seenKeys, unprocessedProperties) {
        const { isKubernetes } = options;
        const additional = schema.additionalProperties;
        if (typeof additional === 'object') {
            for (const propertyName of unprocessedProperties) {
                const child = seenKeys[propertyName];
                if (!child)
                    continue;
                validationResult.evaluatedProperties ?? (validationResult.evaluatedProperties = new Set());
                validationResult.evaluatedProperties.add(propertyName);
                const propertyValidationResult = new ValidationResult(isKubernetes);
                this.validateNode(child, additional, schema, propertyValidationResult, matchingSchemas, options);
                validationResult.mergePropertyMatch(propertyValidationResult, false);
                validationResult.mergeEnumValues(propertyValidationResult);
            }
            return;
        }
        if (additional === true) {
            if (unprocessedProperties.length > 0) {
                validationResult.evaluatedProperties ?? (validationResult.evaluatedProperties = new Set());
                for (const propertyName of unprocessedProperties) {
                    if (seenKeys[propertyName]) {
                        validationResult.evaluatedProperties.add(propertyName);
                    }
                }
            }
            return;
        }
        const forbidExtra = additional === false ||
            (schema.type === 'object' && additional === undefined && options.disableAdditionalProperties === true);
        if (!forbidExtra)
            return;
        if (unprocessedProperties.length === 0)
            return;
        const possibleProperties = schema.properties &&
            Object.entries(schema.properties)
                .filter(([key, property]) => {
                if (seenKeys[key])
                    return false;
                if (property && typeof property === 'object' && (property.doNotSuggest || property.deprecationMessage))
                    return false;
                return true;
            })
                .map(([key]) => key);
        for (const propertyName of unprocessedProperties) {
            const child = seenKeys[propertyName];
            if (!child)
                continue;
            let propertyNode = child.type === 'property' ? child : (child.parent ?? child);
            if (propertyNode.type === 'object') {
                propertyNode = propertyNode.properties[0] ?? propertyNode;
            }
            const keyNode = propertyNode.type === 'property' ? propertyNode.keyNode : propertyNode;
            const problem = {
                location: { offset: keyNode.offset, length: keyNode.length },
                severity: DiagnosticSeverity.Warning,
                code: ErrorCode.PropertyExpected,
                message: schema.errorMessage || l10n.t('Property {0} is not allowed.', propertyName),
                source: this.getSchemaSource(schema, originalSchema),
                schemaUri: this.getSchemaUri(schema, originalSchema),
            };
            if (possibleProperties?.length)
                problem.data = { properties: possibleProperties };
            validationResult.problems.push(problem);
        }
    }
    applyPropertyCount(node, schema, originalSchema, validationResult) {
        if (isNumber(schema.maxProperties) && node.properties.length > (schema.maxProperties ?? 0)) {
            validationResult.problems.push({
                location: { offset: node.offset, length: node.length },
                severity: DiagnosticSeverity.Warning,
                message: l10n.t('Object has more properties than limit of {0}.', schema.maxProperties),
                source: this.getSchemaSource(schema, originalSchema),
                schemaUri: this.getSchemaUri(schema, originalSchema),
            });
        }
        if (isNumber(schema.minProperties) && node.properties.length < (schema.minProperties ?? 0)) {
            validationResult.problems.push({
                location: { offset: node.offset, length: node.length },
                severity: DiagnosticSeverity.Warning,
                message: l10n.t('Object has fewer properties than the required number of {0}', schema.minProperties),
                source: this.getSchemaSource(schema, originalSchema),
                schemaUri: this.getSchemaUri(schema, originalSchema),
            });
        }
    }
    applyDependencies(node, schema, originalSchema, validationResult, matchingSchemas, options, seenKeys) {
        // Draft-07 behavior: dependencies keyword
        const { isKubernetes } = options;
        const deps = schema.dependencies;
        if (!deps)
            return;
        for (const key of Object.keys(deps)) {
            const prop = seenKeys[key];
            if (!prop)
                continue;
            const propertyDep = deps[key];
            if (Array.isArray(propertyDep)) {
                for (const requiredProp of propertyDep) {
                    if (!seenKeys[requiredProp]) {
                        validationResult.problems.push({
                            location: { offset: node.offset, length: node.length },
                            severity: DiagnosticSeverity.Warning,
                            message: l10n.t('Object is missing property {0} required by property {1}.', requiredProp, key),
                            source: this.getSchemaSource(schema, originalSchema),
                            schemaUri: this.getSchemaUri(schema, originalSchema),
                        });
                    }
                    else {
                        validationResult.propertiesValueMatches++;
                    }
                }
            }
            else {
                const propertySchema = asSchema(propertyDep);
                if (propertySchema) {
                    const propertyValidationResult = new ValidationResult(isKubernetes);
                    this.validateNode(node, propertySchema, schema, propertyValidationResult, matchingSchemas, options);
                    validationResult.mergePropertyMatch(propertyValidationResult);
                    validationResult.mergeEnumValues(propertyValidationResult);
                }
            }
        }
    }
    applyPropertyNames(node, schema, validationResult, options) {
        const propertyNames = asSchema(schema.propertyNames);
        if (!propertyNames)
            return;
        for (const f of node.properties) {
            const key = f.keyNode;
            if (key) {
                this.validateNode(key, propertyNames, schema, validationResult, NoOpSchemaCollector.instance, options);
            }
        }
    }
    // ---------------- unevaluated hooks (2019/2020) ----------------
    applyUnevaluatedProperties(
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    _node, 
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    _schema, 
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    _originalSchema, 
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    _validationResult, 
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    _matchingSchemas, 
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    _options, 
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    _seenKeys, 
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    _unprocessedProperties) {
        // no-op in draft-07
    }
    applyUnevaluatedItems(
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    _node, 
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    _schema, 
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    _originalSchema, 
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    _validationResult, 
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    _matchingSchemas, 
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    _options) {
        // no-op in draft-07
    }
    // ---------------- best-match helpers ----------------
    alternativeComparison(subValidationResult, bestMatch, subSchema, subMatchingSchemas) {
        const compareResult = subValidationResult.compareKubernetes(bestMatch.validationResult);
        if (compareResult > 0) {
            return { schema: subSchema, validationResult: subValidationResult, matchingSchemas: subMatchingSchemas };
        }
        if (compareResult === 0) {
            bestMatch.matchingSchemas.merge(subMatchingSchemas);
            bestMatch.validationResult.mergeEnumValues(subValidationResult);
        }
        return bestMatch;
    }
    genericComparison(node, maxOneMatch, subValidationResult, bestMatch, subSchema, subMatchingSchemas) {
        if (!maxOneMatch && !subValidationResult.hasProblems() && !bestMatch.validationResult.hasProblems()) {
            bestMatch.matchingSchemas.merge(subMatchingSchemas);
            bestMatch.validationResult.propertiesMatches += subValidationResult.propertiesMatches;
            bestMatch.validationResult.propertiesValueMatches += subValidationResult.propertiesValueMatches;
        }
        else {
            const compareResult = subValidationResult.compareGeneric(bestMatch.validationResult);
            if (compareResult > 0 ||
                (compareResult === 0 &&
                    maxOneMatch &&
                    bestMatch.schema.type === 'object' &&
                    node.type !== 'null' &&
                    node.type !== bestMatch.schema.type)) {
                bestMatch = { schema: subSchema, validationResult: subValidationResult, matchingSchemas: subMatchingSchemas };
            }
            else if (compareResult === 0 || ((node.value === null || node.type === 'null') && node.length === 0)) {
                this.mergeValidationMatches(bestMatch, subMatchingSchemas, subValidationResult);
            }
        }
        return bestMatch;
    }
    mergeValidationMatches(bestMatch, subMatchingSchemas, subValidationResult) {
        bestMatch.matchingSchemas.merge(subMatchingSchemas);
        bestMatch.validationResult.mergeEnumValues(subValidationResult);
        bestMatch.validationResult.mergeWarningGeneric(subValidationResult, [
            ProblemType.missingRequiredPropWarning,
            ProblemType.typeMismatchWarning,
            ProblemType.constWarning,
        ]);
    }
}
export function asSchema(schema) {
    if (schema === undefined)
        return undefined;
    if (isBoolean(schema))
        return schema ? {} : { not: {} };
    if (typeof schema !== 'object') {
        // Keep legacy behavior: warn and coerce
        // eslint-disable-next-line no-console
        console.warn(`Wrong schema: ${JSON.stringify(schema)}, it MUST be an Object or Boolean`);
        return { type: schema };
    }
    return schema;
}
function getWarningMessage(problemType, args) {
    return l10n.t(ProblemTypeMessages[problemType], args.join(' | '));
}
function isAutoCompleteEqualMaybe(callFromAutoComplete, node, nodeValue, schemaValue) {
    if (!callFromAutoComplete)
        return false;
    const isWithoutValue = nodeValue === null && node.length === 0; // allows `prop: ` but ignore `prop: null`
    if (isWithoutValue)
        return true;
    return isString(nodeValue) && isString(schemaValue) && schemaValue.startsWith(nodeValue);
}
//# sourceMappingURL=baseValidator.js.map