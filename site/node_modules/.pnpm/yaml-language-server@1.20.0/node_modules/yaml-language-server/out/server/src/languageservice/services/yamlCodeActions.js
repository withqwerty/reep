"use strict";
/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Red Hat, Inc. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/
Object.defineProperty(exports, "__esModule", { value: true });
exports.YamlCodeActions = void 0;
const l10n = require("@vscode/l10n");
const path = require("path");
const vscode_json_languageservice_1 = require("vscode-json-languageservice");
const vscode_languageserver_types_1 = require("vscode-languageserver-types");
const yaml_1 = require("yaml");
const commands_1 = require("../../commands");
const textBuffer_1 = require("../utils/textBuffer");
const yamlScalar_1 = require("../utils/yamlScalar");
const baseValidator_1 = require("../parser/schemaValidation/baseValidator");
const strings_1 = require("../utils/strings");
const arrUtils_1 = require("../utils/arrUtils");
const yaml_documents_1 = require("../parser/yaml-documents");
const block_string_rewriter_1 = require("../utils/block-string-rewriter");
const flow_style_rewriter_1 = require("../utils/flow-style-rewriter");
class YamlCodeActions {
    constructor(clientCapabilities) {
        this.clientCapabilities = clientCapabilities;
        this.indentation = '  ';
        this.lineWidth = 80;
    }
    configure(settings, printWidth) {
        this.indentation = settings.indentation;
        this.lineWidth = printWidth;
    }
    getCodeAction(document, params) {
        if (!params.context.diagnostics) {
            return;
        }
        const result = [];
        result.push(...this.getConvertToBooleanActions(params.context.diagnostics, document));
        result.push(...this.getJumpToSchemaActions(params.context.diagnostics));
        result.push(...this.getTabToSpaceConverting(params.context.diagnostics, document));
        result.push(...this.getUnusedAnchorsDelete(params.context.diagnostics, document));
        result.push(...this.getConvertToBlockStyleActions(params.context.diagnostics, document));
        result.push(...this.getConvertStringToBlockStyleActions(params.range, document));
        result.push(...this.getKeyOrderActions(params.context.diagnostics, document));
        result.push(...this.getQuickFixForPropertyOrValueMismatch(params.context.diagnostics, document));
        return result;
    }
    getJumpToSchemaActions(diagnostics) {
        const isOpenTextDocumentEnabled = this.clientCapabilities?.window?.showDocument?.support ?? false;
        if (!isOpenTextDocumentEnabled) {
            return [];
        }
        const schemaUriToDiagnostic = new Map();
        for (const diagnostic of diagnostics) {
            const schemaUri = diagnostic.data?.schemaUri || [];
            for (const schemaUriStr of schemaUri) {
                if (schemaUriStr) {
                    if (!schemaUriToDiagnostic.has(schemaUriStr)) {
                        schemaUriToDiagnostic.set(schemaUriStr, []);
                    }
                    schemaUriToDiagnostic.get(schemaUriStr).push(diagnostic);
                }
            }
        }
        const result = [];
        for (const schemaUri of schemaUriToDiagnostic.keys()) {
            const action = vscode_languageserver_types_1.CodeAction.create(l10n.t('Jump to schema location ({0})', path.basename(schemaUri)), vscode_languageserver_types_1.Command.create('JumpToSchema', commands_1.YamlCommands.JUMP_TO_SCHEMA, schemaUri));
            action.diagnostics = schemaUriToDiagnostic.get(schemaUri);
            result.push(action);
        }
        return result;
    }
    getTabToSpaceConverting(diagnostics, document) {
        const result = [];
        const textBuff = new textBuffer_1.TextBuffer(document);
        const processedLine = [];
        for (const diag of diagnostics) {
            if (diag.message === 'Tabs are not allowed as indentation') {
                if (processedLine.includes(diag.range.start.line)) {
                    continue;
                }
                const lineContent = textBuff.getLineContent(diag.range.start.line);
                let replacedTabs = 0;
                let newText = '';
                for (let i = diag.range.start.character; i <= diag.range.end.character; i++) {
                    const char = lineContent.charAt(i);
                    if (char !== '\t') {
                        break;
                    }
                    replacedTabs++;
                    newText += this.indentation;
                }
                processedLine.push(diag.range.start.line);
                let resultRange = diag.range;
                if (replacedTabs !== diag.range.end.character - diag.range.start.character) {
                    resultRange = vscode_languageserver_types_1.Range.create(diag.range.start, vscode_languageserver_types_1.Position.create(diag.range.end.line, diag.range.start.character + replacedTabs));
                }
                result.push(vscode_languageserver_types_1.CodeAction.create(l10n.t('Convert Tab to Spaces'), createWorkspaceEdit(document.uri, [vscode_languageserver_types_1.TextEdit.replace(resultRange, newText)]), vscode_languageserver_types_1.CodeActionKind.QuickFix));
            }
        }
        if (result.length !== 0) {
            const replaceEdits = [];
            for (let i = 0; i <= textBuff.getLineCount(); i++) {
                const lineContent = textBuff.getLineContent(i);
                let replacedTabs = 0;
                let newText = '';
                for (let j = 0; j < lineContent.length; j++) {
                    const char = lineContent.charAt(j);
                    if (char !== ' ' && char !== '\t') {
                        if (replacedTabs !== 0) {
                            replaceEdits.push(vscode_languageserver_types_1.TextEdit.replace(vscode_languageserver_types_1.Range.create(i, j - replacedTabs, i, j), newText));
                            replacedTabs = 0;
                            newText = '';
                        }
                        break;
                    }
                    if (char === ' ' && replacedTabs !== 0) {
                        replaceEdits.push(vscode_languageserver_types_1.TextEdit.replace(vscode_languageserver_types_1.Range.create(i, j - replacedTabs, i, j), newText));
                        replacedTabs = 0;
                        newText = '';
                        continue;
                    }
                    if (char === '\t') {
                        newText += this.indentation;
                        replacedTabs++;
                    }
                }
                // line contains only tabs
                if (replacedTabs !== 0) {
                    replaceEdits.push(vscode_languageserver_types_1.TextEdit.replace(vscode_languageserver_types_1.Range.create(i, 0, i, textBuff.getLineLength(i)), newText));
                }
            }
            if (replaceEdits.length > 0) {
                result.push(vscode_languageserver_types_1.CodeAction.create(l10n.t('Convert all Tabs to Spaces'), createWorkspaceEdit(document.uri, replaceEdits), vscode_languageserver_types_1.CodeActionKind.QuickFix));
            }
        }
        return result;
    }
    getUnusedAnchorsDelete(diagnostics, document) {
        const result = [];
        const buffer = new textBuffer_1.TextBuffer(document);
        for (const diag of diagnostics) {
            if (diag.message.startsWith('Unused anchor') && diag.source === baseValidator_1.YAML_SOURCE) {
                const range = vscode_languageserver_types_1.Range.create(diag.range.start, diag.range.end);
                const actual = buffer.getText(range);
                const lineContent = buffer.getLineContent(range.end.line);
                const lastWhitespaceChar = (0, strings_1.getFirstNonWhitespaceCharacterAfterOffset)(lineContent, range.end.character);
                range.end.character = lastWhitespaceChar;
                const action = vscode_languageserver_types_1.CodeAction.create(l10n.t('Delete unused anchor: {0}', actual), createWorkspaceEdit(document.uri, [vscode_languageserver_types_1.TextEdit.del(range)]), vscode_languageserver_types_1.CodeActionKind.QuickFix);
                action.diagnostics = [diag];
                result.push(action);
            }
        }
        return result;
    }
    getConvertToBooleanActions(diagnostics, document) {
        const results = [];
        for (const diagnostic of diagnostics) {
            if (diagnostic.message === 'Incorrect type. Expected "boolean".') {
                const value = document.getText(diagnostic.range).toLocaleLowerCase();
                if (value === '"true"' || value === '"false"' || value === "'true'" || value === "'false'") {
                    const newValue = value.includes('true') ? 'true' : 'false';
                    results.push(vscode_languageserver_types_1.CodeAction.create(l10n.t('Convert to boolean'), createWorkspaceEdit(document.uri, [vscode_languageserver_types_1.TextEdit.replace(diagnostic.range, newValue)]), vscode_languageserver_types_1.CodeActionKind.QuickFix));
                }
            }
        }
        return results;
    }
    getConvertToBlockStyleActions(diagnostics, document) {
        const results = [];
        for (const diagnostic of diagnostics) {
            if (diagnostic.code === 'flowMap' || diagnostic.code === 'flowSeq') {
                const node = getNodeForDiagnostic(document, diagnostic);
                if ((0, yaml_1.isMap)(node.internalNode) || (0, yaml_1.isSeq)(node.internalNode)) {
                    const blockTypeDescription = (0, yaml_1.isMap)(node.internalNode) ? 'map' : 'sequence';
                    const rewriter = new flow_style_rewriter_1.FlowStyleRewriter(this.indentation);
                    results.push(vscode_languageserver_types_1.CodeAction.create(l10n.t('Convert to block style {0}', blockTypeDescription), createWorkspaceEdit(document.uri, [vscode_languageserver_types_1.TextEdit.replace(diagnostic.range, rewriter.write(node))]), vscode_languageserver_types_1.CodeActionKind.QuickFix));
                }
            }
        }
        return results;
    }
    getConvertStringToBlockStyleActions(range, document) {
        const yamlDocument = yaml_documents_1.yamlDocumentsCache.getYamlDocument(document);
        const startOffset = range ? document.offsetAt(range.start) : 0;
        const endOffset = range ? document.offsetAt(range.end) : Infinity;
        const results = [];
        for (const singleYamlDocument of yamlDocument.documents) {
            const matchingNodes = [];
            (0, yaml_1.visit)(singleYamlDocument.internalDocument, (key, node) => {
                if ((0, yaml_1.isScalar)(node)) {
                    if ((startOffset <= node.range[0] && node.range[2] <= endOffset) ||
                        (node.range[0] <= startOffset && endOffset <= node.range[2])) {
                        if (node.type === 'QUOTE_DOUBLE' || node.type === 'QUOTE_SINGLE') {
                            if (typeof node.value === 'string' && (node.value.indexOf('\n') >= 0 || node.value.length > this.lineWidth)) {
                                matchingNodes.push(node);
                            }
                        }
                    }
                }
            });
            for (const node of matchingNodes) {
                const range = vscode_languageserver_types_1.Range.create(document.positionAt(node.range[0]), document.positionAt(node.range[1]));
                const rewriter = new block_string_rewriter_1.BlockStringRewriter(this.indentation, this.lineWidth);
                const foldedBlockScalar = rewriter.writeFoldedBlockScalar(node);
                if (foldedBlockScalar !== null) {
                    results.push(vscode_languageserver_types_1.CodeAction.create(l10n.t('Convert string to folded block string'), createWorkspaceEdit(document.uri, [vscode_languageserver_types_1.TextEdit.replace(range, foldedBlockScalar)]), vscode_languageserver_types_1.CodeActionKind.Refactor));
                }
                const literalBlockScalar = rewriter.writeLiteralBlockScalar(node);
                if (literalBlockScalar !== null) {
                    results.push(vscode_languageserver_types_1.CodeAction.create(l10n.t('Convert string to literal block string'), createWorkspaceEdit(document.uri, [vscode_languageserver_types_1.TextEdit.replace(range, literalBlockScalar)]), vscode_languageserver_types_1.CodeActionKind.Refactor));
                }
            }
        }
        return results;
    }
    getKeyOrderActions(diagnostics, document) {
        const results = [];
        for (const diagnostic of diagnostics) {
            if (diagnostic?.code === 'mapKeyOrder') {
                let node = getNodeForDiagnostic(document, diagnostic);
                while (node && node.type !== 'object') {
                    node = node.parent;
                }
                if (node && (0, yaml_1.isMap)(node.internalNode)) {
                    const sorted = structuredClone(node.internalNode);
                    const _getTrailingTokens = (value) => {
                        if (!value)
                            return;
                        if (yaml_1.CST.isScalar(value)) {
                            if (value.type === 'block-scalar') {
                                value.props ?? (value.props = []);
                                return value.props;
                            }
                            value.end ?? (value.end = []);
                            return value.end;
                        }
                        if (value.type === 'flow-collection') {
                            value.end ?? (value.end = []);
                            return value.end;
                        }
                        if (value.type === 'block-map') {
                            const lastItem = value.items[value.items.length - 1];
                            return lastItem ? _getTrailingTokens(lastItem.value) : undefined;
                        }
                        return;
                    };
                    if ((sorted.srcToken.type === 'block-map' || sorted.srcToken.type === 'flow-collection') &&
                        (node.internalNode.srcToken.type === 'block-map' || node.internalNode.srcToken.type === 'flow-collection')) {
                        sorted.srcToken.items.sort((a, b) => {
                            if (a.key && b.key && yaml_1.CST.isScalar(a.key) && yaml_1.CST.isScalar(b.key)) {
                                return a.key.source.localeCompare(b.key.source);
                            }
                            if (!a.key && b.key) {
                                return -1;
                            }
                            if (a.key && !b.key) {
                                return 1;
                            }
                            if (!a.key && !b.key) {
                                return 0;
                            }
                        });
                        for (let i = 0; i < sorted.srcToken.items.length; i++) {
                            const item = sorted.srcToken.items[i];
                            const uItem = node.internalNode.srcToken.items[i];
                            item.start = uItem.start;
                            // strip leading blank lines so reordered entries stay one-per-line
                            while (item.start?.[0]?.type === 'newline')
                                item.start.shift();
                            const itemTokens = _getTrailingTokens(item.value);
                            const itemNewLineIndex = itemTokens?.findIndex((p) => p.type === 'newline') ?? -1;
                            const uNewLineToken = _getTrailingTokens(uItem.value)?.find((p) => p.type === 'newline' && p.offset < node.offset + node.length) ??
                                null;
                            if (uNewLineToken && itemNewLineIndex < 0 && itemTokens) {
                                itemTokens.push({ type: 'newline', indent: 0, offset: item.value.offset, source: '\n' });
                            }
                            if (!uNewLineToken && itemNewLineIndex > -1 && itemTokens) {
                                itemTokens.splice(itemNewLineIndex, 1);
                            }
                        }
                    }
                    const replaceRange = vscode_languageserver_types_1.Range.create(document.positionAt(node.offset), document.positionAt(node.offset + node.length));
                    results.push(vscode_languageserver_types_1.CodeAction.create(l10n.t('Fix key order for this map'), createWorkspaceEdit(document.uri, [vscode_languageserver_types_1.TextEdit.replace(replaceRange, yaml_1.CST.stringify(sorted.srcToken))]), vscode_languageserver_types_1.CodeActionKind.QuickFix));
                }
            }
        }
        return results;
    }
    /**
     * Check if diagnostic contains info for quick fix
     * Supports Enum/Const/Property mismatch
     */
    getPossibleQuickFixValues(diagnostic) {
        if (typeof diagnostic.data !== 'object') {
            return;
        }
        if (diagnostic.code === vscode_json_languageservice_1.ErrorCode.EnumValueMismatch &&
            'values' in diagnostic.data &&
            Array.isArray(diagnostic.data.values)) {
            return diagnostic.data.values;
        }
        else if (diagnostic.code === vscode_json_languageservice_1.ErrorCode.PropertyExpected &&
            'properties' in diagnostic.data &&
            Array.isArray(diagnostic.data.properties)) {
            return diagnostic.data.properties;
        }
    }
    getQuickFixForPropertyOrValueMismatch(diagnostics, document) {
        const results = [];
        for (const diagnostic of diagnostics) {
            const values = this.getPossibleQuickFixValues(diagnostic);
            if (!values?.length) {
                continue;
            }
            for (const value of values) {
                const scalar = typeof value === 'string' ? (0, yamlScalar_1.toYamlStringScalar)(value) : String(value);
                results.push(vscode_languageserver_types_1.CodeAction.create(scalar, createWorkspaceEdit(document.uri, [vscode_languageserver_types_1.TextEdit.replace(diagnostic.range, scalar)]), vscode_languageserver_types_1.CodeActionKind.QuickFix));
            }
        }
        return results;
    }
}
exports.YamlCodeActions = YamlCodeActions;
function getNodeForDiagnostic(document, diagnostic) {
    const yamlDocuments = yaml_documents_1.yamlDocumentsCache.getYamlDocument(document);
    const startOffset = document.offsetAt(diagnostic.range.start);
    const endOffset = document.offsetAt(diagnostic.range.end);
    const yamlDoc = (0, arrUtils_1.matchOffsetToDocument)(startOffset, yamlDocuments);
    let node = yamlDoc.getNodeFromOffset(startOffset);
    if (node && startOffset < endOffset && node.offset + node.length === startOffset) {
        const nodeInsideRange = yamlDoc.getNodeFromOffset(startOffset + 1);
        if (nodeInsideRange) {
            node = nodeInsideRange;
        }
    }
    return node;
}
function createWorkspaceEdit(uri, edits) {
    const changes = {};
    changes[uri] = edits;
    const edit = {
        changes,
    };
    return edit;
}
//# sourceMappingURL=yamlCodeActions.js.map