import { findNodeAtOffset } from './astNodeUtils';
import { getValidator } from './schemaValidation/validatorFactory';
class ASTNodeImpl {
    constructor(parent, internalNode, offset, length) {
        this.offset = offset;
        this.length = length;
        this.parent = parent;
        this.internalNode = internalNode;
    }
    getNodeFromOffsetEndInclusive(offset) {
        const collector = [];
        const findNode = (node) => {
            if (offset >= node.offset && offset <= node.offset + node.length) {
                const children = node.children;
                for (let i = 0; i < children.length && children[i].offset <= offset; i++) {
                    const item = findNode(children[i]);
                    if (item) {
                        collector.push(item);
                    }
                }
                return node;
            }
            return null;
        };
        const foundNode = findNode(this);
        let currMinDist = Number.MAX_VALUE;
        let currMinNode = null;
        for (const currNode of collector) {
            const minDist = currNode.length + currNode.offset - offset + (offset - currNode.offset);
            if (minDist < currMinDist) {
                currMinNode = currNode;
                currMinDist = minDist;
            }
        }
        return currMinNode || foundNode;
    }
    get children() {
        return [];
    }
    toString() {
        return ('type: ' +
            this.type +
            ' (' +
            this.offset +
            '/' +
            this.length +
            ')' +
            (this.parent ? ' parent: {' + this.parent.toString() + '}' : ''));
    }
}
export class NullASTNodeImpl extends ASTNodeImpl {
    constructor(parent, internalNode, offset, length) {
        super(parent, internalNode, offset, length);
        this.type = 'null';
        this.value = null;
    }
}
export class BooleanASTNodeImpl extends ASTNodeImpl {
    constructor(parent, internalNode, boolValue, boolSource, offset, length) {
        super(parent, internalNode, offset, length);
        this.type = 'boolean';
        this.value = boolValue;
        this.source = boolSource;
    }
}
export class ArrayASTNodeImpl extends ASTNodeImpl {
    constructor(parent, internalNode, offset, length) {
        super(parent, internalNode, offset, length);
        this.type = 'array';
        this.items = [];
    }
    get children() {
        return this.items;
    }
}
export class NumberASTNodeImpl extends ASTNodeImpl {
    constructor(parent, internalNode, offset, length) {
        super(parent, internalNode, offset, length);
        this.type = 'number';
        this.isInteger = true;
        this.value = Number.NaN;
    }
}
export class StringASTNodeImpl extends ASTNodeImpl {
    constructor(parent, internalNode, offset, length) {
        super(parent, internalNode, offset, length);
        this.type = 'string';
        this.value = '';
    }
}
export class PropertyASTNodeImpl extends ASTNodeImpl {
    constructor(parent, internalNode, offset, length) {
        super(parent, internalNode, offset, length);
        this.type = 'property';
        this.colonOffset = -1;
    }
    get children() {
        return this.valueNode ? [this.keyNode, this.valueNode] : [this.keyNode];
    }
}
export class ObjectASTNodeImpl extends ASTNodeImpl {
    constructor(parent, internalNode, offset, length) {
        super(parent, internalNode, offset, length);
        this.type = 'object';
        this.properties = [];
    }
    get children() {
        return this.properties;
    }
}
export var EnumMatch;
(function (EnumMatch) {
    EnumMatch[EnumMatch["Key"] = 0] = "Key";
    EnumMatch[EnumMatch["Enum"] = 1] = "Enum";
})(EnumMatch || (EnumMatch = {}));
export function newJSONDocument(root, diagnostics = []) {
    return new JSONDocument(root, diagnostics, []);
}
export class JSONDocument {
    constructor(root, syntaxErrors = [], comments = []) {
        this.root = root;
        this.syntaxErrors = syntaxErrors;
        this.comments = comments;
    }
    getNodeFromOffset(offset, includeRightBound = false) {
        if (this.root) {
            return findNodeAtOffset(this.root, offset, includeRightBound);
        }
        return undefined;
    }
    getNodeFromOffsetEndInclusive(offset) {
        return this.root && this.root.getNodeFromOffsetEndInclusive(offset);
    }
    visit(visitor) {
        if (this.root) {
            const doVisit = (node) => {
                let ctn = visitor(node);
                const children = node.children;
                if (Array.isArray(children)) {
                    for (let i = 0; i < children.length && ctn; i++) {
                        ctn = doVisit(children[i]);
                    }
                }
                return ctn;
            };
            doVisit(this.root);
        }
    }
    validate(textDocument, schema) {
        if (!this.root || !schema)
            return null;
        const validator = getValidator(schema._dialect);
        return validator.validateDocument(this.root, textDocument, schema, {
            isKubernetes: this.isKubernetes,
            disableAdditionalProperties: this.disableAdditionalProperties,
            uri: this.uri,
        });
    }
    getMatchingSchemas(schema, focusOffset = -1, exclude = null, didCallFromAutoComplete) {
        if (!this.root || !schema)
            return [];
        const validator = getValidator(schema._dialect);
        return validator.getMatchingSchemas(this.root, schema, {
            isKubernetes: this.isKubernetes,
            disableAdditionalProperties: this.disableAdditionalProperties,
            uri: this.uri,
            callFromAutoComplete: didCallFromAutoComplete,
        }, focusOffset, exclude);
    }
}
//# sourceMappingURL=jsonDocument.js.map