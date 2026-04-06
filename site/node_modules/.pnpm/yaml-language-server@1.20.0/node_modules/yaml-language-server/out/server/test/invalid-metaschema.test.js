"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
/*---------------------------------------------------------------------------------------------
 *  Copyright (c) IBM Corp. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/
const chai_1 = require("chai");
const sinon = require("sinon");
const schemaSelectionHandlers_1 = require("../src/languageserver/handlers/schemaSelectionHandlers");
const yamlSchemaService_1 = require("../src/languageservice/services/yamlSchemaService");
const yamlSettings_1 = require("../src/yamlSettings");
const testHelper_1 = require("./utils/testHelper");
describe('unexpected meta schema', () => {
    const sandbox = sinon.createSandbox();
    const connection = {};
    let service;
    let requestServiceMock;
    beforeEach(() => {
        requestServiceMock = sandbox.fake.resolves('{ "$schema": "https://example.com/my-custom-meta-schema/v1", "type": "object" }');
        service = new yamlSchemaService_1.YAMLSchemaService(requestServiceMock);
        connection.client = {};
        const onRequest = sandbox.fake();
        connection.onRequest = onRequest;
    });
    afterEach(() => {
        sandbox.restore();
    });
    it('should not throw when a non-standard meta schema is used', async () => {
        service.registerExternalSchema('https://some.com/some.json', ['*.yaml'], undefined, 'Schema name', 'Schema description');
        const settings = new yamlSettings_1.SettingsState();
        const testTextDocument = (0, testHelper_1.setupSchemaIDTextDocument)('');
        settings.documents = new yamlSettings_1.TextDocumentTestManager();
        settings.documents.set(testTextDocument);
        const selection = new schemaSelectionHandlers_1.JSONSchemaSelection(service, settings, connection);
        try {
            await selection.getSchemas(testTextDocument.uri);
        }
        catch (e) {
            chai_1.assert.fail('Unexpected exception: ' + e);
        }
    });
});
//# sourceMappingURL=invalid-metaschema.test.js.map