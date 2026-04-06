import { TextDocument } from 'vscode-languageserver-textdocument';
import { Range, WorkspaceEdit } from 'vscode-languageserver-types';
import { Telemetry } from '../telemetry';
import { PrepareRenameParams, RenameParams } from 'vscode-languageserver-protocol';
export declare class YamlRename {
    private readonly telemetry?;
    constructor(telemetry?: Telemetry);
    prepareRename(document: TextDocument, params: PrepareRenameParams): Range | null;
    doRename(document: TextDocument, params: RenameParams): WorkspaceEdit | null;
    private findTarget;
    private findByToken;
    private findAnchorToken;
    private getAnchorFromToken;
    private getAnchorFromCollectionItem;
    private getNameRange;
    private isOffsetInsideToken;
    private findInvalidAnchorChar;
}
