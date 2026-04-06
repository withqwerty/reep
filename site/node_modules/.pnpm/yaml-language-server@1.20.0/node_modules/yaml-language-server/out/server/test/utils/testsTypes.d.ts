import { Disposable, Event, NotificationHandler, RequestHandler } from 'vscode-jsonrpc';
import { ApplyWorkspaceEditParams, WorkspaceEdit, ApplyWorkspaceEditResponse, ConfigurationItem, WorkspaceFolder, WorkspaceFoldersChangeEvent, CreateFilesParams, RenameFilesParams, DeleteFilesParams, ClientCapabilities, ServerCapabilities } from 'vscode-languageserver-protocol';
import { Connection, RemoteWorkspace } from 'vscode-languageserver';
import { TelemetryImpl } from '../../src/languageserver/telemetry';
import { TelemetryEvent } from '../../src/languageservice/telemetry';
export declare class TestWorkspace implements RemoteWorkspace {
    connection: Connection;
    applyEdit(paramOrEdit: ApplyWorkspaceEditParams | WorkspaceEdit): Promise<ApplyWorkspaceEditResponse>;
    fillServerCapabilities(capabilities: ServerCapabilities<any>): void;
    getConfiguration(): Promise<any>;
    getConfiguration(section: string): Promise<any>;
    getConfiguration(item: ConfigurationItem): Promise<any>;
    getConfiguration(items: ConfigurationItem[]): Promise<any[]>;
    getWorkspaceFolders(): Promise<WorkspaceFolder[]>;
    initialize(capabilities: ClientCapabilities): void;
    onDidChangeWorkspaceFolders: Event<WorkspaceFoldersChangeEvent>;
    onDidCreateFiles(handler: NotificationHandler<CreateFilesParams>): Disposable;
    onDidRenameFiles(handler: NotificationHandler<RenameFilesParams>): Disposable;
    onDidDeleteFiles(handler: NotificationHandler<DeleteFilesParams>): Disposable;
    onWillCreateFiles(handler: RequestHandler<CreateFilesParams, WorkspaceEdit, never>): Disposable;
    onWillRenameFiles(handler: RequestHandler<RenameFilesParams, WorkspaceEdit, never>): Disposable;
    onWillDeleteFiles(handler: RequestHandler<DeleteFilesParams, WorkspaceEdit, never>): Disposable;
}
export declare class TestTelemetry extends TelemetryImpl {
    messages: TelemetryEvent[];
    constructor(connection: Connection);
    send(event: TelemetryEvent): void;
    clearMessages(): void;
}
