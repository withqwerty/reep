import { JSONSchema } from '../jsonSchema';
import { SchemaPriority, SchemaRequestService, WorkspaceContextService } from '../yamlLanguageService';
import { SettingsState } from '../../yamlSettings';
import { UnresolvedSchema, ResolvedSchema, JSONSchemaService, SchemaDependencies, ISchemaContributions, SchemaHandle } from 'vscode-json-languageservice/lib/umd/services/jsonSchemaService';
import { JSONDocument } from '../parser/jsonDocument';
import { JSONSchemaDescriptionExt } from '../../requestTypes';
import { SchemaVersions } from '../yamlTypes';
export declare type CustomSchemaProvider = (uri: string) => Promise<string | string[]>;
export declare enum MODIFICATION_ACTIONS {
    'delete' = 0,
    'add' = 1,
    'deleteAll' = 2
}
export interface SchemaAdditions {
    schema: string;
    action: MODIFICATION_ACTIONS.add;
    path: string;
    key: string;
    content: any;
}
export interface SchemaDeletions {
    schema: string;
    action: MODIFICATION_ACTIONS.delete;
    path: string;
    key: string;
}
export interface SchemaDeletionsAll {
    schemas: string[];
    action: MODIFICATION_ACTIONS.deleteAll;
}
export declare class YAMLSchemaService extends JSONSchemaService {
    [x: string]: any;
    private customSchemaProvider;
    private filePatternAssociations;
    private contextService;
    private requestService;
    private yamlSettings;
    schemaPriorityMapping: Map<string, Set<SchemaPriority>>;
    private schemaUriToNameAndDescription;
    constructor(requestService: SchemaRequestService, contextService?: WorkspaceContextService, promiseConstructor?: PromiseConstructor, yamlSettings?: SettingsState);
    registerCustomSchemaProvider(customSchemaProvider: CustomSchemaProvider): void;
    getAllSchemas(): JSONSchemaDescriptionExt[];
    private collectSchemaNodes;
    private schemaMapValues;
    resolveSchemaContent(schemaToResolve: UnresolvedSchema, schemaURL: string, dependencies: SchemaDependencies): Promise<ResolvedSchema>;
    getSchemaForResource(resource: string, doc: JSONDocument): Promise<ResolvedSchema>;
    private finalizeResolvedSchema;
    addSchemaPriority(uri: string, priority: number): void;
    /**
     * Search through all the schemas and find the ones with the highest priority
     */
    private highestPrioritySchemas;
    private resolveCustomSchema;
    /**
     * Save a schema with schema ID and schema content.
     * Overrides previous schemas set for that schema ID.
     */
    saveSchema(schemaId: string, schemaContent: JSONSchema): Promise<void>;
    /**
     * Delete schemas on specific path
     */
    deleteSchemas(deletions: SchemaDeletionsAll): Promise<void>;
    /**
     * Delete a schema with schema ID.
     */
    deleteSchema(schemaId: string): Promise<void>;
    /**
     * Add content to a specified schema at a specified path
     */
    addContent(additions: SchemaAdditions): Promise<void>;
    /**
     * Delete content in a specified schema at a specified path
     */
    deleteContent(deletions: SchemaDeletions): Promise<void>;
    /**
     * Take a JSON Schema and the path that you would like to get to
     * @returns the JSON Schema resolved at that specific path
     */
    private resolveJSONSchemaToSection;
    /**
     * Resolve the next Object if they have compatible types
     * @param object a location in the JSON Schema
     * @param token the next token that you want to search for
     */
    private resolveNext;
    /**
     * Everything below here is needed because we're importing from vscode-json-languageservice umd and we need
     * to provide a wrapper around the javascript methods we are calling since they have no type
     */
    normalizeId(id: string): string;
    getOrAddSchemaHandle(id: string, unresolvedSchemaContent?: JSONSchema): SchemaHandle;
    loadSchema(schemaUri: string): Promise<UnresolvedSchema>;
    registerExternalSchema(uri: string, filePatterns?: string[], unresolvedSchema?: JSONSchema, name?: string, description?: string, versions?: SchemaVersions): SchemaHandle;
    clearExternalSchemas(): void;
    setSchemaContributions(schemaContributions: ISchemaContributions): void;
    getRegisteredSchemaIds(filter?: (scheme: any) => boolean): string[];
    getResolvedSchema(schemaId: string): Promise<ResolvedSchema>;
    onResourceChange(uri: string): boolean;
}
