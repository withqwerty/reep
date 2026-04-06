"use strict";
/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Red Hat, Inc. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/
Object.defineProperty(exports, "__esModule", { value: true });
const sinon = require("sinon");
const sinonChai = require("sinon-chai");
const chai = require("chai");
const yamlCommands_1 = require("../src/languageservice/services/yamlCommands");
const commandExecutor_1 = require("../src/languageserver/commandExecutor");
const vscode_uri_1 = require("vscode-uri");
const expect = chai.expect;
chai.use(sinonChai);
describe('Yaml Commands', () => {
    const JSON_SCHEMA_LOCAL = 'file://some/path/schema.json';
    const sandbox = sinon.createSandbox();
    let commandExecutorStub;
    beforeEach(() => {
        commandExecutorStub = sandbox.stub(commandExecutor_1.commandExecutor, 'registerCommand');
    });
    afterEach(() => {
        sandbox.restore();
    });
    it('should register handler for "JumpToSchema" command', () => {
        (0, yamlCommands_1.registerCommands)(commandExecutor_1.commandExecutor, {});
        expect(commandExecutorStub).to.have.been.calledWithMatch(sinon.match('jumpToSchema'), sinon.match.func);
    });
    it('JumpToSchema handler should call "showDocument"', async () => {
        const showDocumentStub = sandbox.stub();
        const getWorkspaceFoldersStub = sandbox.stub().returns(Promise.resolve([]));
        const connection = {
            window: {
                showDocument: showDocumentStub,
            },
            workspace: {
                getWorkspaceFolders: getWorkspaceFoldersStub,
            },
        };
        showDocumentStub.resolves(true);
        (0, yamlCommands_1.registerCommands)(commandExecutor_1.commandExecutor, connection);
        const arg = commandExecutorStub.args[0];
        await arg[1](JSON_SCHEMA_LOCAL);
        expect(showDocumentStub).to.have.been.calledWith({ uri: JSON_SCHEMA_LOCAL, external: false, takeFocus: true });
    });
    it('JumpToSchema handler should call "showDocument" with plain win path', async () => {
        const showDocumentStub = sandbox.stub();
        const getWorkspaceFoldersStub = sandbox.stub().returns(Promise.resolve([]));
        const connection = {
            window: {
                showDocument: showDocumentStub,
            },
            workspace: {
                getWorkspaceFolders: getWorkspaceFoldersStub,
            },
        };
        showDocumentStub.resolves(true);
        (0, yamlCommands_1.registerCommands)(commandExecutor_1.commandExecutor, connection);
        const arg = commandExecutorStub.args[0];
        await arg[1]('a:\\some\\path\\to\\schema.json');
        expect(showDocumentStub).to.have.been.calledWith({
            uri: vscode_uri_1.URI.file('a:\\some\\path\\to\\schema.json').toString(),
            external: false,
            takeFocus: true,
        });
    });
    it('JumpToSchema handler should call "showDocument" with custom web schema', async () => {
        const showDocumentStub = sandbox.stub();
        const getWorkspaceFoldersStub = sandbox.stub().returns(Promise.resolve([{ uri: 'vscode-test:///root/' }]));
        const connection = {
            window: {
                showDocument: showDocumentStub,
            },
            workspace: {
                getWorkspaceFolders: getWorkspaceFoldersStub,
            },
        };
        showDocumentStub.resolves(true);
        (0, yamlCommands_1.registerCommands)(commandExecutor_1.commandExecutor, connection);
        const arg = commandExecutorStub.args[0];
        await arg[1]('my-file.json');
        expect(showDocumentStub).to.have.been.calledWith({
            uri: vscode_uri_1.URI.parse('vscode-test:///root/my-file.json').toString(),
            external: false,
            takeFocus: true,
        });
    });
});
//# sourceMappingURL=yamlCommands.test.js.map