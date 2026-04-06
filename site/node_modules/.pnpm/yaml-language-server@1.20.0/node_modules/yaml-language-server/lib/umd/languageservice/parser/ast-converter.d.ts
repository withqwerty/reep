import { Alias, Document, LineCounter } from 'yaml';
import { ASTNode, YamlNode } from '../jsonASTTypes';
type NodeRange = [number, number, number];
export declare const aliasDepth: {
    maxRefCount: number;
    currentRefDepth: number;
    aliasResolutionCache: Map<Alias, ASTNode>;
};
export declare function convertAST(parent: ASTNode, node: YamlNode, doc: Document, lineCounter: LineCounter): ASTNode | undefined;
export declare function toOffsetLength(range: NodeRange): [number, number];
export {};
