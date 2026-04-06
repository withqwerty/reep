import type { ASTNode } from '../jsonASTTypes';
export declare function getNodeValue(node: ASTNode): any;
export declare function contains(node: ASTNode, offset: number, includeRightBound?: boolean): boolean;
export declare function findNodeAtOffset(node: ASTNode, offset: number, includeRightBound: boolean): ASTNode;
