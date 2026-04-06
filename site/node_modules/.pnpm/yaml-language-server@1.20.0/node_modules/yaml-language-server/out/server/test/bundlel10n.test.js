"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Red Hat. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/
const l10n = require("@vscode/l10n");
const assert = require("assert");
const path = require("path");
const node_1 = require("vscode-languageserver/node");
const schemaRequestHandler_1 = require("../src/languageservice/services/schemaRequestHandler");
const nodeTranslationSetup_1 = require("../src/nodeTranslationSetup");
const yamlServerInit_1 = require("../src/yamlServerInit");
const yamlSettings_1 = require("../src/yamlSettings");
const testHelper_1 = require("./utils/testHelper");
const testsTypes_1 = require("./utils/testsTypes");
describe('Bundle l10n Test', () => {
    let serverInit;
    before(() => {
        const yamlSettings = new yamlSettings_1.SettingsState();
        process.argv.push('--node-ipc');
        const connection = (0, node_1.createConnection)();
        const schemaRequestHandlerWrapper = (connection, uri) => {
            const testSchemaProvider = testHelper_1.TestCustomSchemaProvider.instance();
            const testSchema = testSchemaProvider.getContentForSchema(uri);
            if (testSchema) {
                return Promise.resolve(testSchema);
            }
            return (0, schemaRequestHandler_1.schemaRequestHandler)(connection, uri, yamlSettings.workspaceFolders, yamlSettings.workspaceRoot, yamlSettings.useVSCodeContentRequest, testHelper_1.testFileSystem, false);
        };
        const schemaRequestService = schemaRequestHandlerWrapper.bind(this, connection);
        const telemetry = new testsTypes_1.TestTelemetry(connection);
        serverInit = new yamlServerInit_1.YAMLServerInit(connection, yamlSettings, schemaRequestHandler_1.workspaceContext, schemaRequestService, telemetry, nodeTranslationSetup_1.setupl10nBundle);
    });
    after(async () => {
        await serverInit.setupl10nBundle({
            locale: 'en',
            processId: 0,
            rootUri: '',
            capabilities: undefined,
            initializationOptions: {
                l10nPath: path.join(__dirname, '../l10n'),
            },
        });
    });
    describe('l10n bundle test', function () {
        it('check french locale', async () => {
            await serverInit.setupl10nBundle({
                locale: 'fr',
                processId: 0,
                rootUri: '',
                capabilities: undefined,
                initializationOptions: {
                    l10nPath: path.join(__dirname, '../l10n'),
                },
            });
            assert.equal(l10n.t('Default value'), 'Valeur par défaut');
        });
        it('un configured locale should return in english', async () => {
            await serverInit.setupl10nBundle({
                locale: 'pt-br',
                processId: 0,
                rootUri: '',
                capabilities: undefined,
                initializationOptions: {
                    l10nPath: path.join(__dirname, '../l10n'),
                },
            });
            assert.equal(l10n.t('Default value'), 'Default value');
        });
    });
});
//# sourceMappingURL=bundlel10n.test.js.map