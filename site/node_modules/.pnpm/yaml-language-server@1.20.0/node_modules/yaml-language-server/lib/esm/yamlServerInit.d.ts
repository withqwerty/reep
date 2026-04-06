import { Connection, InitializeParams, InitializeResult } from 'vscode-languageserver';
import { LanguageHandlers } from './languageserver/handlers/languageHandlers';
import { SettingsHandler } from './languageserver/handlers/settingsHandlers';
import { ValidationHandler } from './languageserver/handlers/validationHandlers';
import { Telemetry } from './languageservice/telemetry';
import { LanguageService, SchemaRequestService, WorkspaceContextService } from './languageservice/yamlLanguageService';
import { SettingsState } from './yamlSettings';
export declare class YAMLServerInit {
    private readonly connection;
    private yamlSettings;
    private workspaceContext;
    private schemaRequestService;
    private telemetry;
    setupl10nBundle: (params: InitializeParams) => Promise<void>;
    languageService: LanguageService;
    languageHandler: LanguageHandlers;
    validationHandler: ValidationHandler;
    settingsHandler: SettingsHandler;
    constructor(connection: Connection, yamlSettings: SettingsState, workspaceContext: WorkspaceContextService, schemaRequestService: SchemaRequestService, telemetry: Telemetry, setupl10nBundle?: (params: InitializeParams) => Promise<void>);
    connectionInitialized(params: InitializeParams): Promise<InitializeResult>;
    private registerHandlers;
    start(): void;
}
