import type * as vscode from 'vscode-languageserver-protocol';
import { type URI } from 'vscode-uri';
import type { LanguageServiceContext } from '../types';
export declare function register(context: LanguageServiceContext): (uri: URI, range: vscode.Range | undefined, legend: vscode.SemanticTokensLegend, _reportProgress?: (tokens: vscode.SemanticTokens) => void, // TODO
token?: vscode.CancellationToken) => Promise<vscode.SemanticTokens | undefined>;
