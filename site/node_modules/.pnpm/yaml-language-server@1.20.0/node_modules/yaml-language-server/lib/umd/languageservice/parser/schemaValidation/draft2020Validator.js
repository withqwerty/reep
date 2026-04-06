/*---------------------------------------------------------------------------------------------
 *  Copyright (c) IBM Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/
(function (factory) {
    if (typeof module === "object" && typeof module.exports === "object") {
        var v = factory(require, exports);
        if (v !== undefined) module.exports = v;
    }
    else if (typeof define === "function" && define.amd) {
        define(["require", "exports", "../../jsonSchema", "../../utils/objects", "@vscode/l10n", "vscode-languageserver-types", "./draft2019Validator", "./baseValidator"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    exports.Draft2020Validator = void 0;
    const jsonSchema_1 = require("../../jsonSchema");
    const objects_1 = require("../../utils/objects");
    const l10n = require("@vscode/l10n");
    const vscode_languageserver_types_1 = require("vscode-languageserver-types");
    const draft2019Validator_1 = require("./draft2019Validator");
    const baseValidator_1 = require("./baseValidator");
    class Draft2020Validator extends draft2019Validator_1.Draft2019Validator {
        getCurrentDialect() {
            return jsonSchema_1.SchemaDialect.draft2020;
        }
        /**
         * Keyword: prefixItems + items
         */
        validateArrayNode(node, schema, originalSchema, validationResult, matchingSchemas, options) {
            const items = (node.items ?? []);
            // prefixItems/items/contains contribute to evaluatedItems
            const evaluatedItems = validationResult.getEvaluatedItems(node);
            const prefixItems = schema.prefixItems;
            // validate prefixItems
            if (Array.isArray(prefixItems)) {
                const limit = Math.min(prefixItems.length, items.length);
                for (let i = 0; i < limit; i++) {
                    const subSchema = (0, baseValidator_1.asSchema)(prefixItems[i]);
                    if (!subSchema) {
                        evaluatedItems.add(i);
                        continue;
                    }
                    const itemValidationResult = new baseValidator_1.ValidationResult(options.isKubernetes);
                    this.validateNode(items[i], subSchema, schema, itemValidationResult, matchingSchemas, options);
                    validationResult.mergePropertyMatch(itemValidationResult, false);
                    validationResult.mergeEnumValues(itemValidationResult);
                    // mark as evaluated even if invalid (avoids duplicate unevaluatedItems noise)
                    evaluatedItems.add(i);
                }
            }
            // validate remaining items against items
            const itemsKeyword = schema.items;
            const prefixLen = Array.isArray(prefixItems) ? prefixItems.length : 0;
            if (items.length > prefixLen) {
                if (itemsKeyword === false) {
                    // "items": false => no items allowed beyond prefixItems
                    validationResult.problems.push({
                        location: { offset: node.offset, length: node.length },
                        severity: vscode_languageserver_types_1.DiagnosticSeverity.Warning,
                        message: l10n.t('Array has too many items according to schema. Expected {0} or fewer.', prefixLen),
                        source: this.getSchemaSource(schema, originalSchema),
                        schemaUri: this.getSchemaUri(schema, originalSchema),
                    });
                    // mark these as evaluated by "items": false (so unevaluatedItems doesn't also complain)
                    for (let i = prefixLen; i < items.length; i++) {
                        evaluatedItems.add(i);
                    }
                }
                else {
                    const tailSchema = (0, baseValidator_1.asSchema)(itemsKeyword);
                    // if items is undefined, there's no constraint for remaining items and they remain unevaluated
                    if (tailSchema) {
                        for (let i = prefixLen; i < items.length; i++) {
                            const itemValidationResult = new baseValidator_1.ValidationResult(options.isKubernetes);
                            this.validateNode(items[i], tailSchema, schema, itemValidationResult, matchingSchemas, options);
                            validationResult.mergePropertyMatch(itemValidationResult, false);
                            validationResult.mergeEnumValues(itemValidationResult);
                            // mark as evaluated even if invalid (avoids duplicate unevaluatedItems noise)
                            evaluatedItems.add(i);
                        }
                    }
                }
            }
            // contains enforces min/max and marks matching indices as evaluated
            this.applyContains(node, schema, originalSchema, validationResult, matchingSchemas, options);
            // generic array keywords
            this.applyArrayLength(node, schema, originalSchema, validationResult, options);
            this.applyUniqueItems(node, schema, originalSchema, validationResult);
        }
        /**
         * Draft 2020-12: contains keyword affects the unevaluatedItems keyword
         */
        applyContains(node, schema, originalSchema, validationResult, _matchingSchemas, options) {
            const containsSchema = (0, baseValidator_1.asSchema)(schema.contains);
            if (!containsSchema)
                return;
            const items = (node.items ?? []);
            const minContainsRaw = schema.minContains;
            const maxContainsRaw = schema.maxContains;
            const minContains = (0, objects_1.isNumber)(minContainsRaw) ? minContainsRaw : 1;
            const maxContains = (0, objects_1.isNumber)(maxContainsRaw) ? maxContainsRaw : undefined;
            let matchCount = 0;
            // ensure evaluatedItems exists
            const evaluatedItems = validationResult.getEvaluatedItems(node);
            for (let i = 0; i < items.length; i++) {
                const itemValidationResult = new baseValidator_1.ValidationResult(options.isKubernetes);
                this.validateNode(items[i], containsSchema, schema, itemValidationResult, this.getNoOpCollector(), options);
                if (!itemValidationResult.hasProblems()) {
                    // items that match contains are considered evaluated
                    evaluatedItems.add(i);
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
    }
    exports.Draft2020Validator = Draft2020Validator;
});
//# sourceMappingURL=draft2020Validator.js.map