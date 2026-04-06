"use strict";
/*---------------------------------------------------------------------------------------------
 *  Copyright (c) IBM Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/
Object.defineProperty(exports, "__esModule", { value: true });
exports.Draft2019Validator = void 0;
const jsonSchema_1 = require("../../jsonSchema");
const objects_1 = require("../../utils/objects");
const l10n = require("@vscode/l10n");
const vscode_languageserver_types_1 = require("vscode-languageserver-types");
const vscode_json_languageservice_1 = require("vscode-json-languageservice");
const draft07Validator_1 = require("./draft07Validator");
const baseValidator_1 = require("./baseValidator");
class Draft2019Validator extends draft07Validator_1.Draft07Validator {
    getCurrentDialect() {
        return jsonSchema_1.SchemaDialect.draft2019;
    }
    /**
     * Keyword: contains + minContains/maxContains
     *
     * Draft-07 behavior: contains must match at least 1 item.
     * Draft-2019-09 behavior: minContains/maxContains constrain how many matches are required/allowed.
     */
    applyContains(node, schema, originalSchema, validationResult, _matchingSchemas, options) {
        const containsSchema = (0, baseValidator_1.asSchema)(schema.contains);
        if (!containsSchema)
            return;
        const minContainsRaw = schema.minContains;
        const maxContainsRaw = schema.maxContains;
        const minContains = (0, objects_1.isNumber)(minContainsRaw) ? minContainsRaw : 1;
        const maxContains = (0, objects_1.isNumber)(maxContainsRaw) ? maxContainsRaw : undefined;
        let matchCount = 0;
        const items = (node.items ?? []);
        for (const item of items) {
            const itemValidationResult = new baseValidator_1.ValidationResult(options.isKubernetes);
            this.validateNode(item, containsSchema, schema, itemValidationResult, this.getNoOpCollector(), options);
            if (!itemValidationResult.hasProblems()) {
                matchCount++;
                if (maxContains !== undefined && matchCount > maxContains) {
                    break;
                }
            }
        }
        if (matchCount < minContains) {
            validationResult.problems.push({
                location: { offset: node.offset, length: node.length },
                severity: vscode_languageserver_types_1.DiagnosticSeverity.Warning,
                message: schema.errorMessage || l10n.t('Array has too few items matching "contains". Expected {0} or more.', minContains),
                source: this.getSchemaSource(schema, originalSchema),
                schemaUri: this.getSchemaUri(schema, originalSchema),
            });
        }
        if (maxContains !== undefined && matchCount > maxContains) {
            validationResult.problems.push({
                location: { offset: node.offset, length: node.length },
                severity: vscode_languageserver_types_1.DiagnosticSeverity.Warning,
                message: schema.errorMessage || l10n.t('Array has too many items matching "contains". Expected {0} or fewer.', maxContains),
                source: this.getSchemaSource(schema, originalSchema),
                schemaUri: this.getSchemaUri(schema, originalSchema),
            });
        }
    }
    /**
     * Keyword: dependentRequired + dependentSchemas.
     */
    applyDependencies(node, schema, originalSchema, validationResult, matchingSchemas, options, seenKeys) {
        // keep draft-07 dependencies support
        super.applyDependencies(node, schema, originalSchema, validationResult, matchingSchemas, options, seenKeys);
        const dependentRequired = schema.dependentRequired;
        if (dependentRequired && typeof dependentRequired === 'object') {
            for (const prop of Object.keys(dependentRequired)) {
                if (!seenKeys[prop])
                    continue;
                const requiredProps = dependentRequired[prop];
                if (!Array.isArray(requiredProps))
                    continue;
                for (const requiredProp of requiredProps) {
                    if (!seenKeys[requiredProp]) {
                        validationResult.problems.push({
                            location: { offset: node.offset, length: node.length },
                            severity: vscode_languageserver_types_1.DiagnosticSeverity.Warning,
                            message: l10n.t('Object is missing property {0} required by property {1}.', requiredProp, prop),
                            source: this.getSchemaSource(schema, originalSchema),
                            schemaUri: this.getSchemaUri(schema, originalSchema),
                        });
                    }
                    else {
                        validationResult.propertiesValueMatches++;
                    }
                }
            }
        }
        const dependentSchemas = schema.dependentSchemas;
        if (dependentSchemas && typeof dependentSchemas === 'object') {
            for (const prop of Object.keys(dependentSchemas)) {
                if (!seenKeys[prop])
                    continue;
                const depSchema = (0, baseValidator_1.asSchema)(dependentSchemas[prop]);
                if (!depSchema)
                    continue;
                const depValidationResult = new baseValidator_1.ValidationResult(options.isKubernetes);
                this.validateNode(node, depSchema, schema, depValidationResult, matchingSchemas, options);
                validationResult.mergePropertyMatch(depValidationResult);
                validationResult.mergeEnumValues(depValidationResult);
            }
        }
    }
    /**
     * Keyword: unevaluatedProperties
     */
    applyUnevaluatedProperties(node, schema, originalSchema, validationResult, matchingSchemas, options, seenKeys) {
        const unevaluated = schema.unevaluatedProperties;
        if (unevaluated === undefined)
            return;
        if (!seenKeys)
            return;
        // ensure evaluatedProperties exists
        validationResult.evaluatedProperties ?? (validationResult.evaluatedProperties = new Set());
        // remaining = properties not evaluated by properties/patternProperties/additionalProperties
        const remaining = Object.keys(seenKeys).filter((name) => !validationResult.evaluatedProperties?.has(name));
        if (remaining.length === 0)
            return;
        // unevaluatedProperties: false => forbid remaining properties
        if (unevaluated === false) {
            for (const propName of remaining) {
                const child = seenKeys[propName];
                if (!child)
                    continue;
                const propertyNode = child.type === 'property' ? child : child.parent;
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                const keyNode = propertyNode.keyNode;
                if (!keyNode)
                    continue;
                validationResult.problems.push({
                    location: { offset: keyNode.offset, length: keyNode.length },
                    severity: vscode_languageserver_types_1.DiagnosticSeverity.Warning,
                    code: vscode_json_languageservice_1.ErrorCode.PropertyExpected,
                    message: schema.errorMessage || l10n.t('Property {0} is not allowed.', propName),
                    source: this.getSchemaSource(schema, originalSchema),
                    schemaUri: this.getSchemaUri(schema, originalSchema),
                });
                validationResult.evaluatedProperties?.add(propName);
            }
            return;
        }
        // unevaluatedProperties: true => allow anything remaining, but mark evaluated
        if (unevaluated === true) {
            for (const propName of remaining) {
                validationResult.evaluatedProperties?.add(propName);
            }
            return;
        }
        // unevaluatedProperties: <schema> => validate value of each remaining property
        const unevaluatedSchema = (0, baseValidator_1.asSchema)(unevaluated);
        if (!unevaluatedSchema)
            return;
        for (const propName of remaining) {
            const child = seenKeys[propName];
            if (!child)
                continue;
            const valueNode = child.type === 'property' ? child.valueNode : child;
            if (!valueNode)
                continue;
            const subResult = new baseValidator_1.ValidationResult(options.isKubernetes);
            this.validateNode(valueNode, unevaluatedSchema, schema, subResult, matchingSchemas, options);
            validationResult.mergePropertyMatch(subResult);
            validationResult.mergeEnumValues(subResult);
            validationResult.evaluatedProperties?.add(propName);
        }
    }
    /**
     * Keyword: unevaluatedItems
     */
    applyUnevaluatedItems(node, schema, originalSchema, validationResult, matchingSchemas, options) {
        const unevaluated = schema.unevaluatedItems;
        if (unevaluated === undefined)
            return;
        const items = (node.items ?? []);
        if (items.length === 0)
            return;
        const evaluated = validationResult.getEvaluatedItems(node);
        const remaining = [];
        for (let i = 0; i < items.length; i++) {
            if (!evaluated.has(i))
                remaining.push(i);
        }
        if (remaining.length === 0)
            return;
        // unevaluatedItems: false => forbid remaining indices
        if (unevaluated === false) {
            for (const idx of remaining) {
                const item = items[idx];
                validationResult.problems.push({
                    location: { offset: item.offset, length: item.length || 1 },
                    severity: vscode_languageserver_types_1.DiagnosticSeverity.Warning,
                    code: vscode_json_languageservice_1.ErrorCode.PropertyExpected,
                    message: schema.errorMessage || l10n.t('Array has too many items according to schema. Expected {0} or fewer.', idx),
                    source: this.getSchemaSource(schema, originalSchema),
                    schemaUri: this.getSchemaUri(schema, originalSchema),
                });
                evaluated.add(idx);
            }
            return;
        }
        // unevaluatedItems: true => allow everything remaining, but mark evaluated
        if (unevaluated === true) {
            for (const idx of remaining)
                evaluated.add(idx);
            return;
        }
        // unevaluatedItems: <schema> => validate remaining items against that schema
        const unevaluatedSchema = (0, baseValidator_1.asSchema)(unevaluated);
        if (!unevaluatedSchema)
            return;
        for (const idx of remaining) {
            const item = items[idx];
            const subResult = new baseValidator_1.ValidationResult(options.isKubernetes);
            // validate the item node with the unevaluatedItems subschema
            this.validateNode(item, unevaluatedSchema, schema, subResult, matchingSchemas, options);
            validationResult.merge(subResult);
            validationResult.mergeEnumValues(subResult);
            evaluated.add(idx);
        }
    }
}
exports.Draft2019Validator = Draft2019Validator;
//# sourceMappingURL=draft2019Validator.js.map