"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const chai_1 = require("chai");
const vscode_languageserver_types_1 = require("vscode-languageserver-types");
const testHelper_1 = require("./utils/testHelper");
function applyEdits(document, edits) {
    const sorted = [...edits].sort((a, b) => document.offsetAt(b.range.start) - document.offsetAt(a.range.start));
    let content = document.getText();
    for (const edit of sorted) {
        const start = document.offsetAt(edit.range.start);
        const end = document.offsetAt(edit.range.end);
        content = content.slice(0, start) + edit.newText + content.slice(end);
    }
    return content;
}
describe('YAML Rename', () => {
    it('renames anchor and aliases when invoked on alias', () => {
        const { languageService } = (0, testHelper_1.setupLanguageService)({});
        const document = (0, testHelper_1.setupTextDocument)('foo: &a value\nbar: *a\nbaz: *a\n');
        const result = languageService.doRename(document, {
            position: vscode_languageserver_types_1.Position.create(1, 6),
            textDocument: { uri: testHelper_1.TEST_URI },
            newName: 'renamed',
        });
        (0, chai_1.expect)(result).to.not.equal(null);
        const edits = result?.changes?.[testHelper_1.TEST_URI];
        (0, chai_1.expect)(edits).to.have.length(3);
        const updated = applyEdits(document, edits);
        (0, chai_1.expect)(updated).to.equal('foo: &renamed value\nbar: *renamed\nbaz: *renamed\n');
    });
    it('renames when cursor is on anchor token', () => {
        const { languageService } = (0, testHelper_1.setupLanguageService)({});
        const document = (0, testHelper_1.setupTextDocument)('foo: &bar value\nbar: *bar\n');
        const result = languageService.doRename(document, {
            position: vscode_languageserver_types_1.Position.create(0, 6),
            textDocument: { uri: testHelper_1.TEST_URI },
            newName: 'newName',
        });
        (0, chai_1.expect)(result).to.not.equal(null);
        const edits = result?.changes?.[testHelper_1.TEST_URI];
        (0, chai_1.expect)(edits).to.have.length(2);
        const updated = applyEdits(document, edits);
        (0, chai_1.expect)(updated).to.equal('foo: &newName value\nbar: *newName\n');
    });
    it('limits rename to current YAML document', () => {
        const { languageService } = (0, testHelper_1.setupLanguageService)({});
        const document = (0, testHelper_1.setupTextDocument)('---\nfoo: &a 1\nbar: *a\n---\nfoo: &b 1\nbar: *b\n');
        const result = languageService.doRename(document, {
            position: vscode_languageserver_types_1.Position.create(5, 6),
            textDocument: { uri: testHelper_1.TEST_URI },
            newName: 'c',
        });
        (0, chai_1.expect)(result).to.not.equal(null);
        const edits = result?.changes?.[testHelper_1.TEST_URI];
        const updated = applyEdits(document, edits);
        (0, chai_1.expect)(updated).to.equal('---\nfoo: &a 1\nbar: *a\n---\nfoo: &c 1\nbar: *c\n');
    });
    it('returns null for unresolved alias', () => {
        const { languageService } = (0, testHelper_1.setupLanguageService)({});
        const document = (0, testHelper_1.setupTextDocument)('*missing\n');
        const result = languageService.doRename(document, {
            position: vscode_languageserver_types_1.Position.create(0, 1),
            textDocument: { uri: testHelper_1.TEST_URI },
            newName: 'new',
        });
        (0, chai_1.expect)(result).to.equal(null);
    });
    it('prepareRename rejects non-alias/anchor positions', () => {
        const { languageService } = (0, testHelper_1.setupLanguageService)({});
        const document = (0, testHelper_1.setupTextDocument)('foo: bar\n');
        const range = languageService.prepareRename(document, {
            position: vscode_languageserver_types_1.Position.create(0, 1),
            textDocument: { uri: testHelper_1.TEST_URI },
        });
        (0, chai_1.expect)(range).to.equal(null);
    });
    it('rejects invalid anchor names with flow indicator or whitespace characters', () => {
        const { languageService } = (0, testHelper_1.setupLanguageService)({});
        const document = (0, testHelper_1.setupTextDocument)('foo: &bar value\n');
        const invalidNames = ['name[0]', 'name]', 'name{key}', 'name}', 'name,other', 'has space', 'has\ttab'];
        for (const invalidName of invalidNames) {
            (0, chai_1.expect)(() => {
                languageService.doRename(document, {
                    position: vscode_languageserver_types_1.Position.create(0, 6),
                    textDocument: { uri: testHelper_1.TEST_URI },
                    newName: invalidName,
                });
            }).to.throw();
        }
    });
    it('allows anchor names with valid special characters', () => {
        const { languageService } = (0, testHelper_1.setupLanguageService)({});
        const document = (0, testHelper_1.setupTextDocument)('foo: &bar value\nref: *bar\n');
        const result = languageService.doRename(document, {
            position: vscode_languageserver_types_1.Position.create(0, 6),
            textDocument: { uri: testHelper_1.TEST_URI },
            newName: '*anchor&name',
        });
        (0, chai_1.expect)(result).to.not.equal(null);
        const edits = result?.changes?.[testHelper_1.TEST_URI];
        (0, chai_1.expect)(edits).to.have.length(2);
        const updated = applyEdits(document, edits);
        (0, chai_1.expect)(updated).to.equal('foo: &*anchor&name value\nref: **anchor&name\n');
    });
});
//# sourceMappingURL=yamlRename.test.js.map