import { TextDocument } from 'vscode-languageserver-textdocument';
import { Hover, Position } from 'vscode-languageserver-types';
import { Telemetry } from '../telemetry';
import { LanguageSettings } from '../yamlLanguageService';
import { YAMLSchemaService } from './yamlSchemaService';
export declare class YAMLHover {
    private readonly telemetry?;
    private shouldHover;
    private shouldHoverAnchor;
    private indentation;
    private schemaService;
    constructor(schemaService: YAMLSchemaService, telemetry?: Telemetry);
    configure(languageSettings: LanguageSettings): void;
    doHover(document: TextDocument, position: Position, isKubernetes?: boolean): Promise<Hover | null>;
    private getHover;
    /**
     * Resolves merge keys (<<) and anchors recursively in an object node
     * @param node The object AST node to resolve
     * @param doc The YAML document for resolving anchors
     * @param currentRecursionLevel Current recursion level (default: 0)
     * @returns A plain JavaScript object with all merges resolved
     */
    private resolveMergeKeys;
    /**
     * Resolves a merge value (which might be an alias) and recursively resolves its merge keys
     * @param node The AST node that might be an alias or object
     * @param doc The YAML document for resolving anchors
     * @param currentRecursionLevel Current recursion level
     * @returns The resolved value
     */
    private resolveMergeValue;
    /**
     * Converts an AST node to a plain JavaScript value
     * @param node The AST node to convert
     * @param doc The YAML document for resolving anchors
     * @param currentRecursionLevel Current recursion level
     * @returns The converted value
     */
    private astNodeToValue;
    /**
     * Converts a YAML Node to a plain JavaScript value
     * @param node The YAML node to convert
     * @returns The converted value
     */
    private nodeToValue;
    private toMarkdown;
}
