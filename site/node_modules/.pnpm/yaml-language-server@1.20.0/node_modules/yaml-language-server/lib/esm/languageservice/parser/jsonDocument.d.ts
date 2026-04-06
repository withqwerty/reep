import { JSONSchema } from '../jsonSchema';
import { ASTNode, ObjectASTNode, ArrayASTNode, BooleanASTNode, NumberASTNode, StringASTNode, NullASTNode, PropertyASTNode, YamlNode } from '../jsonASTTypes';
import { Diagnostic, Range } from 'vscode-languageserver-types';
import { TextDocument } from 'vscode-languageserver-textdocument';
import { Node, Pair } from 'yaml';
import { type IApplicableSchema } from './schemaValidation/baseValidator';
declare abstract class ASTNodeImpl {
    abstract readonly type: 'object' | 'property' | 'array' | 'number' | 'boolean' | 'null' | 'string';
    offset: number;
    length: number;
    readonly parent: ASTNode;
    location: string;
    readonly internalNode: YamlNode;
    constructor(parent: ASTNode, internalNode: YamlNode, offset: number, length?: number);
    getNodeFromOffsetEndInclusive(offset: number): ASTNode;
    get children(): ASTNode[];
    toString(): string;
}
export declare class NullASTNodeImpl extends ASTNodeImpl implements NullASTNode {
    type: 'null';
    value: any;
    constructor(parent: ASTNode, internalNode: Node, offset: number, length?: number);
}
export declare class BooleanASTNodeImpl extends ASTNodeImpl implements BooleanASTNode {
    type: 'boolean';
    value: boolean;
    source: string;
    constructor(parent: ASTNode, internalNode: Node, boolValue: boolean, boolSource: string, offset: number, length?: number);
}
export declare class ArrayASTNodeImpl extends ASTNodeImpl implements ArrayASTNode {
    type: 'array';
    items: ASTNode[];
    constructor(parent: ASTNode, internalNode: Node, offset: number, length?: number);
    get children(): ASTNode[];
}
export declare class NumberASTNodeImpl extends ASTNodeImpl implements NumberASTNode {
    type: 'number';
    isInteger: boolean;
    value: number;
    constructor(parent: ASTNode, internalNode: Node, offset: number, length?: number);
}
export declare class StringASTNodeImpl extends ASTNodeImpl implements StringASTNode {
    type: 'string';
    value: string;
    constructor(parent: ASTNode, internalNode: Node, offset: number, length?: number);
}
export declare class PropertyASTNodeImpl extends ASTNodeImpl implements PropertyASTNode {
    type: 'property';
    keyNode: StringASTNode;
    valueNode: ASTNode;
    colonOffset: number;
    constructor(parent: ObjectASTNode, internalNode: Pair, offset: number, length?: number);
    get children(): ASTNode[];
}
export declare class ObjectASTNodeImpl extends ASTNodeImpl implements ObjectASTNode {
    type: 'object';
    properties: PropertyASTNode[];
    constructor(parent: ASTNode, internalNode: Node, offset: number, length?: number);
    get children(): ASTNode[];
}
export interface JSONDocumentConfig {
    collectComments?: boolean;
}
export declare enum EnumMatch {
    Key = 0,
    Enum = 1
}
export declare function newJSONDocument(root: ASTNode, diagnostics?: Diagnostic[]): JSONDocument;
export declare class JSONDocument {
    readonly root: ASTNode;
    readonly syntaxErrors: Diagnostic[];
    readonly comments: Range[];
    isKubernetes: boolean;
    disableAdditionalProperties: boolean;
    uri: string;
    constructor(root: ASTNode, syntaxErrors?: Diagnostic[], comments?: Range[]);
    getNodeFromOffset(offset: number, includeRightBound?: boolean): ASTNode | undefined;
    getNodeFromOffsetEndInclusive(offset: number): ASTNode;
    visit(visitor: (node: ASTNode) => boolean): void;
    validate(textDocument: TextDocument, schema: JSONSchema): Diagnostic[];
    getMatchingSchemas(schema: JSONSchema, focusOffset?: number, exclude?: ASTNode, didCallFromAutoComplete?: boolean): IApplicableSchema[];
}
export {};
