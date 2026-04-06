import type * as path from 'typesafe-path/posix';
import type * as ts from 'typescript';
import { URI } from 'vscode-uri';
export declare const defaultCompilerOptions: ts.CompilerOptions;
export declare function asPosix(path: string): path.PosixPath;
export declare const asFileName: (uri: URI) => string;
export declare const asUri: (fileName: string) => URI;
