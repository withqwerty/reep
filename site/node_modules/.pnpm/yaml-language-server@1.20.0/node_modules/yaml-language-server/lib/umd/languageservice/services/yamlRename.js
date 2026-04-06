/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Red Hat, Inc. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/
(function (factory) {
    if (typeof module === "object" && typeof module.exports === "object") {
        var v = factory(require, exports);
        if (v !== undefined) module.exports = v;
    }
    else if (typeof define === "function" && define.amd) {
        define(["require", "exports", "vscode-languageserver-types", "../parser/yaml-documents", "../utils/arrUtils", "../utils/textBuffer", "yaml", "../utils/yamlAstUtils", "vscode-languageserver-protocol"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    exports.YamlRename = void 0;
    const vscode_languageserver_types_1 = require("vscode-languageserver-types");
    const yaml_documents_1 = require("../parser/yaml-documents");
    const arrUtils_1 = require("../utils/arrUtils");
    const textBuffer_1 = require("../utils/textBuffer");
    const yaml_1 = require("yaml");
    const yamlAstUtils_1 = require("../utils/yamlAstUtils");
    const vscode_languageserver_protocol_1 = require("vscode-languageserver-protocol");
    class YamlRename {
        constructor(telemetry) {
            this.telemetry = telemetry;
        }
        prepareRename(document, params) {
            try {
                const target = this.findTarget(document, params.position);
                if (!target) {
                    return null;
                }
                if (!this.findAnchorToken(target.yamlDoc, target.anchorNode)) {
                    return null;
                }
                return this.getNameRange(document, target.token);
            }
            catch (err) {
                this.telemetry?.sendError('yaml.prepareRename.error', err);
                return null;
            }
        }
        doRename(document, params) {
            try {
                const target = this.findTarget(document, params.position);
                if (!target) {
                    return null;
                }
                const anchorToken = this.findAnchorToken(target.yamlDoc, target.anchorNode);
                if (!anchorToken) {
                    return null;
                }
                const newName = params.newName;
                const invalidChar = this.findInvalidAnchorChar(newName);
                if (invalidChar !== null) {
                    throw new vscode_languageserver_protocol_1.ResponseError(vscode_languageserver_protocol_1.ErrorCodes.InvalidParams, `Anchor name cannot contain '${invalidChar}'`);
                }
                const edits = [];
                edits.push(vscode_languageserver_types_1.TextEdit.replace(this.getNameRange(document, anchorToken), newName));
                (0, yaml_1.visit)(target.yamlDoc.internalDocument, (key, node) => {
                    if ((0, yaml_1.isAlias)(node) && node.srcToken && node.resolve(target.yamlDoc.internalDocument) === target.anchorNode) {
                        edits.push(vscode_languageserver_types_1.TextEdit.replace(this.getNameRange(document, node.srcToken), newName));
                    }
                });
                return {
                    changes: {
                        [document.uri]: edits,
                    },
                };
            }
            catch (err) {
                if (err instanceof vscode_languageserver_protocol_1.ResponseError) {
                    throw err;
                }
                this.telemetry?.sendError('yaml.rename.error', err);
                return null;
            }
        }
        findTarget(document, position) {
            const yamlDocuments = yaml_documents_1.yamlDocumentsCache.getYamlDocument(document);
            const offset = document.offsetAt(position);
            const yamlDoc = (0, arrUtils_1.matchOffsetToDocument)(offset, yamlDocuments);
            if (!yamlDoc) {
                return null;
            }
            const [node] = yamlDoc.getNodeFromPosition(offset, new textBuffer_1.TextBuffer(document));
            if (!node) {
                return this.findByToken(yamlDoc, offset);
            }
            if ((0, yaml_1.isAlias)(node) && node.srcToken && this.isOffsetInsideToken(node.srcToken, offset)) {
                const anchorNode = node.resolve(yamlDoc.internalDocument);
                if (!anchorNode) {
                    return null;
                }
                return { anchorNode, token: node.srcToken, yamlDoc };
            }
            if (((0, yaml_1.isCollection)(node) || (0, yaml_1.isScalar)(node)) && node.anchor) {
                const anchorToken = this.findAnchorToken(yamlDoc, node);
                if (anchorToken && this.isOffsetInsideToken(anchorToken, offset)) {
                    return { anchorNode: node, token: anchorToken, yamlDoc };
                }
            }
            return this.findByToken(yamlDoc, offset);
        }
        findByToken(yamlDoc, offset) {
            let target;
            (0, yaml_1.visit)(yamlDoc.internalDocument, (key, node) => {
                if ((0, yaml_1.isAlias)(node) && node.srcToken && this.isOffsetInsideToken(node.srcToken, offset)) {
                    const anchorNode = node.resolve(yamlDoc.internalDocument);
                    if (anchorNode) {
                        target = { anchorNode, token: node.srcToken, yamlDoc };
                        return yaml_1.visit.BREAK;
                    }
                }
                if (((0, yaml_1.isCollection)(node) || (0, yaml_1.isScalar)(node)) && node.anchor) {
                    const anchorToken = this.findAnchorToken(yamlDoc, node);
                    if (anchorToken && this.isOffsetInsideToken(anchorToken, offset)) {
                        target = { anchorNode: node, token: anchorToken, yamlDoc };
                        return yaml_1.visit.BREAK;
                    }
                }
            });
            return target ?? null;
        }
        findAnchorToken(yamlDoc, node) {
            const parent = yamlDoc.getParent(node);
            const candidates = [];
            if (parent && parent.srcToken) {
                candidates.push(parent.srcToken);
            }
            if (node.srcToken) {
                candidates.push(node.srcToken);
            }
            for (const token of candidates) {
                const anchor = this.getAnchorFromToken(token, node);
                if (anchor) {
                    return anchor;
                }
            }
            return undefined;
        }
        getAnchorFromToken(token, node) {
            if ((0, yamlAstUtils_1.isCollectionItem)(token)) {
                return this.getAnchorFromCollectionItem(token);
            }
            else if (yaml_1.CST.isCollection(token)) {
                const collection = token;
                for (const item of collection.items ?? []) {
                    if (item.value !== node.srcToken) {
                        continue;
                    }
                    const anchor = this.getAnchorFromCollectionItem(item);
                    if (anchor) {
                        return anchor;
                    }
                }
            }
            return undefined;
        }
        getAnchorFromCollectionItem(token) {
            for (const t of token.start) {
                if (t.type === 'anchor') {
                    return t;
                }
            }
            if (token.sep && Array.isArray(token.sep)) {
                for (const t of token.sep) {
                    if (t.type === 'anchor') {
                        return t;
                    }
                }
            }
            return undefined;
        }
        getNameRange(document, token) {
            const startOffset = token.offset + 1;
            const endOffset = token.offset + token.source.length;
            return vscode_languageserver_types_1.Range.create(document.positionAt(startOffset), document.positionAt(endOffset));
        }
        isOffsetInsideToken(token, offset) {
            return offset >= token.offset && offset <= token.offset + token.source.length;
        }
        findInvalidAnchorChar(name) {
            // YAML 1.2.2 spec: anchor names cannot contain flow indicators or whitespace
            // https://yaml.org/spec/1.2.2/#rule-ns-anchor-char
            const invalidChars = ['[', ']', '{', '}', ',', ' ', '\t'];
            for (const char of invalidChars) {
                if (name.includes(char)) {
                    return char === ' ' ? 'space' : char === '\t' ? 'tab' : char;
                }
            }
            return null;
        }
    }
    exports.YamlRename = YamlRename;
});
//# sourceMappingURL=yamlRename.js.map