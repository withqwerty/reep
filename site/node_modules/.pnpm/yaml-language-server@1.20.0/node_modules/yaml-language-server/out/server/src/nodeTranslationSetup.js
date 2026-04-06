"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.setupl10nBundle = void 0;
/*---------------------------------------------------------------------------------------------
 *  Copyright (c) IBM Corp. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/
const path = require("path");
const fs_1 = require("fs");
const vscode_uri_1 = require("vscode-uri");
const l10n = require("@vscode/l10n");
/**
 * Loads translations from the filesystem based on the configured locale and the folder of translations provided in the initialization parameters.
 *
 * This is the default implementation when running as binary, but isn't used when running as a web worker.
 *
 * @param params the language server initialization parameters
 */
async function setupl10nBundle(params) {
    const __dirname = path.dirname(__filename);
    const l10nPath = params.initializationOptions?.l10nPath || path.join(__dirname, '../../../l10n');
    const locale = params.locale || 'en';
    if (l10nPath) {
        const bundleFile = !(0, fs_1.existsSync)(path.join(l10nPath, `bundle.l10n.${locale}.json`))
            ? `bundle.l10n.json`
            : `bundle.l10n.${locale}.json`;
        const baseBundleFile = path.join(l10nPath, bundleFile);
        process.env.VSCODE_NLS_CONFIG = JSON.stringify({
            locale,
            _languagePackSupport: true,
        });
        await l10n.config({
            uri: vscode_uri_1.URI.file(baseBundleFile).toString(),
        });
    }
}
exports.setupl10nBundle = setupl10nBundle;
//# sourceMappingURL=nodeTranslationSetup.js.map