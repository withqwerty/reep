/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Red Hat, Inc. All rights reserved.
 *  Copyright (c) Adam Voss. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/
(function (factory) {
    if (typeof module === "object" && typeof module.exports === "object") {
        var v = factory(require, exports);
        if (v !== undefined) module.exports = v;
    }
    else if (typeof define === "function" && define.amd) {
        define(["require", "exports", "vscode-languageserver-types", "prettier/plugins/yaml", "prettier/plugins/estree", "prettier/standalone"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    exports.YAMLFormatter = void 0;
    const vscode_languageserver_types_1 = require("vscode-languageserver-types");
    const yamlPlugin = require("prettier/plugins/yaml");
    const estreePlugin = require("prettier/plugins/estree");
    const standalone_1 = require("prettier/standalone");
    class YAMLFormatter {
        constructor() {
            this.formatterEnabled = true;
        }
        configure(shouldFormat) {
            if (shouldFormat) {
                this.formatterEnabled = shouldFormat.format;
            }
        }
        async format(document, options = {}) {
            if (!this.formatterEnabled) {
                return [];
            }
            try {
                const text = document.getText();
                const prettierOptions = {
                    parser: 'yaml',
                    plugins: [yamlPlugin, estreePlugin],
                    // --- FormattingOptions ---
                    tabWidth: options.tabWidth || options.tabSize,
                    // --- CustomFormatterOptions ---
                    singleQuote: options.singleQuote,
                    bracketSpacing: options.bracketSpacing,
                    // 'preserve' is the default for Options.proseWrap. See also server.ts
                    proseWrap: 'always' === options.proseWrap ? 'always' : 'never' === options.proseWrap ? 'never' : 'preserve',
                    printWidth: options.printWidth,
                    trailingComma: options.trailingComma === false ? 'none' : 'all',
                };
                const formatted = await (0, standalone_1.format)(text, prettierOptions);
                return [vscode_languageserver_types_1.TextEdit.replace(vscode_languageserver_types_1.Range.create(vscode_languageserver_types_1.Position.create(0, 0), document.positionAt(text.length)), formatted)];
            }
            catch (error) {
                return [];
            }
        }
    }
    exports.YAMLFormatter = YAMLFormatter;
});
//# sourceMappingURL=yamlFormatter.js.map