/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Red Hat, Inc. All rights reserved.
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/
(function (factory) {
    if (typeof module === "object" && typeof module.exports === "object") {
        var v = factory(require, exports);
        if (v !== undefined) module.exports = v;
    }
    else if (typeof define === "function" && define.amd) {
        define(["require", "exports", "@vscode/l10n", "path", "vscode-languageserver-types", "vscode-uri", "yaml", "../parser/isKubernetes", "../parser/astNodeUtils", "../parser/yaml-documents", "../utils/arrUtils", "../utils/yamlScalar"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    exports.YAMLHover = void 0;
    const l10n = require("@vscode/l10n");
    const path = require("path");
    const vscode_languageserver_types_1 = require("vscode-languageserver-types");
    const vscode_uri_1 = require("vscode-uri");
    const yaml_1 = require("yaml");
    const isKubernetes_1 = require("../parser/isKubernetes");
    const astNodeUtils_1 = require("../parser/astNodeUtils");
    const yaml_documents_1 = require("../parser/yaml-documents");
    const arrUtils_1 = require("../utils/arrUtils");
    const yamlScalar_1 = require("../utils/yamlScalar");
    class YAMLHover {
        constructor(schemaService, telemetry) {
            this.telemetry = telemetry;
            this.shouldHover = true;
            this.schemaService = schemaService;
        }
        configure(languageSettings) {
            if (languageSettings) {
                this.shouldHover = languageSettings.hover;
                this.shouldHoverAnchor = languageSettings.hoverAnchor;
                this.indentation = languageSettings.indentation;
            }
        }
        doHover(document, position, isKubernetes = false) {
            try {
                if (!this.shouldHover || !document) {
                    return Promise.resolve(undefined);
                }
                const doc = yaml_documents_1.yamlDocumentsCache.getYamlDocument(document);
                const offset = document.offsetAt(position);
                const currentDoc = (0, arrUtils_1.matchOffsetToDocument)(offset, doc);
                if (currentDoc === null) {
                    return Promise.resolve(undefined);
                }
                (0, isKubernetes_1.setKubernetesParserOption)(doc.documents, isKubernetes);
                const currentDocIndex = doc.documents.indexOf(currentDoc);
                currentDoc.currentDocIndex = currentDocIndex;
                return this.getHover(document, position, currentDoc);
            }
            catch (error) {
                this.telemetry?.sendError('yaml.hover.error', error);
            }
        }
        // method copied from https://github.com/microsoft/vscode-json-languageservice/blob/2ea5ad3d2ffbbe40dea11cfe764a502becf113ce/src/services/jsonHover.ts#L23
        getHover(document, position, doc) {
            const offset = document.offsetAt(position);
            let node = doc.getNodeFromOffset(offset);
            if (!node ||
                ((node.type === 'object' || node.type === 'array') && offset > node.offset + 1 && offset < node.offset + node.length - 1)) {
                return Promise.resolve(null);
            }
            const hoverRangeNode = node;
            // use the property description when hovering over an object key
            if (node.type === 'string') {
                const parent = node.parent;
                if (parent && parent.type === 'property' && parent.keyNode === node) {
                    node = parent.valueNode;
                    if (!node) {
                        return Promise.resolve(null);
                    }
                }
            }
            const hoverRange = vscode_languageserver_types_1.Range.create(document.positionAt(hoverRangeNode.offset), document.positionAt(hoverRangeNode.offset + hoverRangeNode.length));
            const createHover = (contents) => {
                const markupContent = {
                    kind: vscode_languageserver_types_1.MarkupKind.Markdown,
                    value: contents,
                };
                const result = {
                    contents: markupContent,
                    range: hoverRange,
                };
                return result;
            };
            if (this.shouldHoverAnchor && node.type === 'property' && node.valueNode) {
                if (node.valueNode.type === 'object') {
                    const resolved = this.resolveMergeKeys(node.valueNode, doc);
                    const contents = '```yaml\n' + (0, yaml_1.stringify)(resolved, null, 2) + '\n```';
                    return Promise.resolve(createHover(contents));
                }
            }
            const removePipe = (value) => {
                return value.replace(/\s\|\|\s*$/, '');
            };
            return this.schemaService.getSchemaForResource(document.uri, doc).then((schema) => {
                if (schema && node && !schema.errors.length) {
                    const matchingSchemas = doc.getMatchingSchemas(schema.schema, node.offset);
                    let title = undefined;
                    let markdownDescription = undefined;
                    let markdownEnumDescriptions = [];
                    const markdownExamples = [];
                    const markdownEnums = [];
                    let enumIdx = undefined;
                    matchingSchemas.every((s) => {
                        if ((s.node === node || (node.type === 'property' && node.valueNode === s.node)) && !s.inverted && s.schema) {
                            title = title || s.schema.title || s.schema.closestTitle;
                            markdownDescription = markdownDescription || s.schema.markdownDescription || this.toMarkdown(s.schema.description);
                            if (s.schema.enum) {
                                enumIdx = s.schema.enum.indexOf((0, astNodeUtils_1.getNodeValue)(node));
                                if (s.schema.markdownEnumDescriptions) {
                                    markdownEnumDescriptions = s.schema.markdownEnumDescriptions;
                                }
                                else if (s.schema.enumDescriptions) {
                                    markdownEnumDescriptions = s.schema.enumDescriptions.map(this.toMarkdown, this);
                                }
                                else {
                                    markdownEnumDescriptions = [];
                                }
                                s.schema.enum.forEach((enumValue, idx) => {
                                    var _a;
                                    enumValue = typeof enumValue === 'string' ? (0, yamlScalar_1.toYamlStringScalar)(enumValue, false) : JSON.stringify(enumValue);
                                    //insert only if the value is not present yet (avoiding duplicates)
                                    //but it also adds or keeps the description of the enum value
                                    const foundIdx = markdownEnums.findIndex((me) => me.value === enumValue);
                                    if (foundIdx < 0) {
                                        markdownEnums.push({
                                            value: enumValue,
                                            description: markdownEnumDescriptions[idx],
                                        });
                                    }
                                    else {
                                        (_a = markdownEnums[foundIdx]).description || (_a.description = markdownEnumDescriptions[idx]);
                                    }
                                });
                            }
                            if (s.schema.anyOf && isAllSchemasMatched(node, matchingSchemas, s.schema)) {
                                //if append title and description of all matched schemas on hover
                                title = '';
                                markdownDescription = s.schema.description ? s.schema.description + '\n' : '';
                                s.schema.anyOf.forEach((childSchema, index) => {
                                    title += childSchema.title || s.schema.closestTitle || '';
                                    markdownDescription += childSchema.markdownDescription || this.toMarkdown(childSchema.description) || '';
                                    if (index !== s.schema.anyOf.length - 1) {
                                        title += ' || ';
                                        markdownDescription += ' || ';
                                    }
                                });
                                title = removePipe(title);
                                markdownDescription = removePipe(markdownDescription);
                            }
                            if (s.schema.examples) {
                                s.schema.examples.forEach((example) => {
                                    markdownExamples.push((0, yaml_1.stringify)(example, null, 2));
                                });
                            }
                        }
                        return true;
                    });
                    let result = '';
                    if (title) {
                        result = '#### ' + this.toMarkdown(title);
                    }
                    if (markdownDescription) {
                        result = ensureLineBreak(result);
                        result += markdownDescription;
                    }
                    if (markdownEnums.length !== 0) {
                        result = ensureLineBreak(result);
                        result += l10n.t('Allowed Values:') + '\n\n';
                        if (enumIdx) {
                            markdownEnums.unshift(markdownEnums.splice(enumIdx, 1)[0]);
                        }
                        markdownEnums.forEach((me) => {
                            if (me.description) {
                                result += `* \`${toMarkdownCodeBlock(me.value)}\`: ${me.description}\n`;
                            }
                            else {
                                result += `* \`${toMarkdownCodeBlock(me.value)}\`\n`;
                            }
                        });
                    }
                    if (markdownExamples.length !== 0) {
                        markdownExamples.forEach((example) => {
                            result = ensureLineBreak(result);
                            result += l10n.t('Example:') + '\n\n';
                            result += `\`\`\`yaml\n${example}\`\`\`\n`;
                        });
                    }
                    if (result.length > 0 && schema.schema.url) {
                        result = ensureLineBreak(result);
                        result += l10n.t('Source: [{0}]({1})', getSchemaName(schema.schema), schema.schema.url);
                    }
                    return createHover(result);
                }
                return null;
            });
        }
        /**
         * Resolves merge keys (<<) and anchors recursively in an object node
         * @param node The object AST node to resolve
         * @param doc The YAML document for resolving anchors
         * @param currentRecursionLevel Current recursion level (default: 0)
         * @returns A plain JavaScript object with all merges resolved
         */
        resolveMergeKeys(node, doc, currentRecursionLevel = 0) {
            const result = {};
            const unprocessedProperties = [...node.properties];
            while (unprocessedProperties.length > 0) {
                const propertyNode = unprocessedProperties.shift();
                const key = propertyNode.keyNode.value;
                if (key === '<<' && propertyNode.valueNode) {
                    // Handle merge key
                    const mergeValue = this.resolveMergeValue(propertyNode.valueNode, doc, currentRecursionLevel + 1);
                    if (mergeValue && typeof mergeValue === 'object' && !Array.isArray(mergeValue)) {
                        // Merge properties from the resolved value
                        const mergeKeys = Object.keys(mergeValue);
                        for (const mergeKey of mergeKeys) {
                            result[mergeKey] = mergeValue[mergeKey];
                        }
                    }
                }
                else {
                    // Regular property
                    result[key] = this.astNodeToValue(propertyNode.valueNode, doc, currentRecursionLevel);
                }
            }
            return result;
        }
        /**
         * Resolves a merge value (which might be an alias) and recursively resolves its merge keys
         * @param node The AST node that might be an alias or object
         * @param doc The YAML document for resolving anchors
         * @param currentRecursionLevel Current recursion level
         * @returns The resolved value
         */
        resolveMergeValue(node, doc, currentRecursionLevel) {
            const MAX_MERGE_RECURSION_LEVEL = 10;
            // Check if we've exceeded max recursion level
            if (currentRecursionLevel >= MAX_MERGE_RECURSION_LEVEL) {
                return { '<<': node.parent.internalNode['value'] + ' (recursion limit reached)' };
            }
            // If it's an object node, resolve its merge keys
            if (node.type === 'object') {
                return this.resolveMergeKeys(node, doc, currentRecursionLevel);
            }
            // Otherwise, convert to value
            return this.astNodeToValue(node, doc, currentRecursionLevel);
        }
        /**
         * Converts an AST node to a plain JavaScript value
         * @param node The AST node to convert
         * @param doc The YAML document for resolving anchors
         * @param currentRecursionLevel Current recursion level
         * @returns The converted value
         */
        astNodeToValue(node, doc, currentRecursionLevel) {
            if (!node) {
                return null;
            }
            switch (node.type) {
                case 'object': {
                    return this.resolveMergeKeys(node, doc, currentRecursionLevel);
                }
                case 'array': {
                    return node.children.map((child) => this.astNodeToValue(child, doc, currentRecursionLevel));
                }
                case 'string':
                case 'number':
                case 'boolean':
                case 'null': {
                    return node.value;
                }
                default: {
                    return this.nodeToValue(node.internalNode);
                }
            }
        }
        /**
         * Converts a YAML Node to a plain JavaScript value
         * @param node The YAML node to convert
         * @returns The converted value
         */
        nodeToValue(node) {
            if ((0, yaml_1.isAlias)(node)) {
                return node.source;
            }
            if ((0, yaml_1.isMap)(node)) {
                const result = {};
                for (const pair of node.items) {
                    if (pair.key && pair.value) {
                        const key = this.nodeToValue(pair.key);
                        const value = this.nodeToValue(pair.value);
                        if (typeof key === 'string') {
                            result[key] = value;
                        }
                    }
                }
                return result;
            }
            if ((0, yaml_1.isSeq)(node)) {
                return node.items.map((item) => this.nodeToValue(item));
            }
            return node.value;
        }
        // copied from https://github.com/microsoft/vscode-json-languageservice/blob/2ea5ad3d2ffbbe40dea11cfe764a502becf113ce/src/services/jsonHover.ts#L112
        toMarkdown(plain) {
            if (plain) {
                let escaped = plain.replace(/([^\n\r])(\r?\n)([^\n\r])/gm, '$1\n\n$3'); // single new lines to \n\n (Markdown paragraph)
                escaped = escaped.replace(/[\\`*_{}[\]#+\-!]/g, '\\$&'); // escape some of the markdown syntax tokens http://daringfireball.net/projects/markdown/syntax#backslash to avoid unintended formatting
                if (this.indentation !== undefined) {
                    // escape indentation whitespace to prevent it from being converted to markdown code blocks.
                    const indentationMatchRegex = new RegExp(` {${this.indentation.length}}`, 'g');
                    escaped = escaped.replace(indentationMatchRegex, '&emsp;');
                }
                return escaped;
            }
            return undefined;
        }
    }
    exports.YAMLHover = YAMLHover;
    function ensureLineBreak(content) {
        if (content.length === 0) {
            return content;
        }
        if (!content.endsWith('\n')) {
            content += '\n';
        }
        return content + '\n';
    }
    function getSchemaName(schema) {
        let result = 'JSON Schema';
        const urlString = schema.url;
        if (urlString) {
            const url = vscode_uri_1.URI.parse(urlString);
            result = path.basename(url.fsPath);
        }
        else if (schema.title) {
            result = schema.title;
        }
        return result;
    }
    // copied from https://github.com/microsoft/vscode-json-languageservice/blob/2ea5ad3d2ffbbe40dea11cfe764a502becf113ce/src/services/jsonHover.ts#L122
    function toMarkdownCodeBlock(content) {
        // see https://daringfireball.net/projects/markdown/syntax#precode
        if (content.indexOf('`') !== -1) {
            return '`` ' + content + ' ``';
        }
        return content;
    }
    /**
     * check all the schemas which is inside anyOf presented or not in matching schema.
     * @param node node
     * @param matchingSchemas all matching schema
     * @param schema scheam which is having anyOf
     * @returns true if all the schemas which inside anyOf presents in matching schema
     */
    function isAllSchemasMatched(node, matchingSchemas, schema) {
        let count = 0;
        for (const matchSchema of matchingSchemas) {
            if (node === matchSchema.node && matchSchema.schema !== schema) {
                schema.anyOf.forEach((childSchema) => {
                    if (matchSchema.schema.title === childSchema.title &&
                        matchSchema.schema.description === childSchema.description &&
                        matchSchema.schema.properties === childSchema.properties) {
                        count++;
                    }
                });
            }
        }
        return count === schema.anyOf.length;
    }
});
//# sourceMappingURL=yamlHover.js.map