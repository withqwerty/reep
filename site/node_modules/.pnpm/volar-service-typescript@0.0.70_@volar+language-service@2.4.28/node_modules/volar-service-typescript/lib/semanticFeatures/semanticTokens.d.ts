import type * as vscode from '@volar/language-service';
import type * as ts from 'typescript';
import type { TextDocument } from 'vscode-languageserver-textdocument';
import type { URI } from 'vscode-uri';
import type { SharedContext } from './types';
export declare function register(ts: typeof import('typescript'), ctx: SharedContext): (uri: URI, document: TextDocument, range: vscode.Range, legend: vscode.SemanticTokensLegend) => [number, number, number, number, number][] | undefined;
export declare function convertClassificationsToSemanticTokens(document: TextDocument, { start, length }: ts.TextSpan, legend: vscode.SemanticTokensLegend, response: ts.Classifications): [number, number, number, number, number][];
//# sourceMappingURL=semanticTokens.d.ts.map