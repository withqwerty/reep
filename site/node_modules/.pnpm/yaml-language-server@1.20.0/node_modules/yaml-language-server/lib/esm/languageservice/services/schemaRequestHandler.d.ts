import { Connection, WorkspaceFolder } from 'vscode-languageserver';
import { URI } from 'vscode-uri';
import { WorkspaceContextService } from '../yamlLanguageService';
export interface FileSystem {
    readFile(fsPath: string, encoding?: string): Promise<string>;
}
/**
 * Handles schema content requests given the schema URI
 * @param uri can be a local file, vscode request, http(s) request or a custom request
 */
export declare const schemaRequestHandler: (connection: Connection, uri: string, workspaceFolders: WorkspaceFolder[], workspaceRoot: URI, useVSCodeContentRequest: boolean, fs: FileSystem, isWeb: boolean) => Promise<string>;
export declare const workspaceContext: WorkspaceContextService;
