"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.findNodeAtOffset = exports.contains = exports.getNodeValue = void 0;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
function getNodeValue(node) {
    switch (node.type) {
        case 'array':
            return node.children.map(getNodeValue);
        case 'object': {
            const obj = Object.create(null);
            for (let _i = 0, _a = node.children; _i < _a.length; _i++) {
                const prop = _a[_i];
                const valueNode = prop.children[1];
                if (valueNode) {
                    obj[prop.children[0].value] = getNodeValue(valueNode);
                }
            }
            return obj;
        }
        case 'null':
        case 'string':
        case 'number':
            return node.value;
        case 'boolean':
            return node.source;
        default:
            return undefined;
    }
}
exports.getNodeValue = getNodeValue;
function contains(node, offset, includeRightBound = false) {
    return ((offset >= node.offset && offset <= node.offset + node.length) || (includeRightBound && offset === node.offset + node.length));
}
exports.contains = contains;
function findNodeAtOffset(node, offset, includeRightBound) {
    if (includeRightBound === void 0) {
        includeRightBound = false;
    }
    if (contains(node, offset, includeRightBound)) {
        const children = node.children;
        if (Array.isArray(children)) {
            for (let i = 0; i < children.length && children[i].offset <= offset; i++) {
                const item = findNodeAtOffset(children[i], offset, includeRightBound);
                if (item) {
                    return item;
                }
            }
        }
        return node;
    }
    return undefined;
}
exports.findNodeAtOffset = findNodeAtOffset;
//# sourceMappingURL=astNodeUtils.js.map