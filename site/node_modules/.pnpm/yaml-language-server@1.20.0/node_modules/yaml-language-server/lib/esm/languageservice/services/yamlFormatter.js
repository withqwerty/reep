/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Red Hat, Inc. All rights reserved.
 *  Copyright (c) Adam Voss. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/
import { Range, Position, TextEdit } from 'vscode-languageserver-types';
import * as yamlPlugin from 'prettier/plugins/yaml';
import * as estreePlugin from 'prettier/plugins/estree';
import { format } from 'prettier/standalone';
export class YAMLFormatter {
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
            const formatted = await format(text, prettierOptions);
            return [TextEdit.replace(Range.create(Position.create(0, 0), document.positionAt(text.length)), formatted)];
        }
        catch (error) {
            return [];
        }
    }
}
//# sourceMappingURL=yamlFormatter.js.map