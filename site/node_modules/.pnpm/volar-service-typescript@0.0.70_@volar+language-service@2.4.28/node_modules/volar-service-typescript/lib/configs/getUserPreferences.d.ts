import type { LanguageServiceContext } from '@volar/language-service';
import type * as ts from 'typescript';
import type { TextDocument } from 'vscode-languageserver-textdocument';
export declare function getUserPreferences(ctx: LanguageServiceContext, document: TextDocument): Promise<ts.UserPreferences>;
//# sourceMappingURL=getUserPreferences.d.ts.map