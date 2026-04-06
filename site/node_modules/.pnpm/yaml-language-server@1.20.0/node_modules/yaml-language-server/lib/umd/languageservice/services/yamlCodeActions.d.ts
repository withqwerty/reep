import { CodeAction } from 'vscode-languageserver-types';
import { ClientCapabilities, CodeActionParams } from 'vscode-languageserver-protocol';
import { TextDocument } from 'vscode-languageserver-textdocument';
import { LanguageSettings } from '../yamlLanguageService';
export declare class YamlCodeActions {
    private readonly clientCapabilities;
    private indentation;
    private lineWidth;
    constructor(clientCapabilities: ClientCapabilities);
    configure(settings: LanguageSettings, printWidth: number): void;
    getCodeAction(document: TextDocument, params: CodeActionParams): CodeAction[] | undefined;
    private getJumpToSchemaActions;
    private getTabToSpaceConverting;
    private getUnusedAnchorsDelete;
    private getConvertToBooleanActions;
    private getConvertToBlockStyleActions;
    private getConvertStringToBlockStyleActions;
    private getKeyOrderActions;
    /**
     * Check if diagnostic contains info for quick fix
     * Supports Enum/Const/Property mismatch
     */
    private getPossibleQuickFixValues;
    private getQuickFixForPropertyOrValueMismatch;
}
