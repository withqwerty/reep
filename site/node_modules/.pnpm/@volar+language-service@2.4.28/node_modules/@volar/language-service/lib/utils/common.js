"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.isInsideRange = isInsideRange;
exports.isEqualRange = isEqualRange;
exports.stringToSnapshot = stringToSnapshot;
exports.sleep = sleep;
function isInsideRange(parent, child) {
    if (child.start.line < parent.start.line) {
        return false;
    }
    if (child.end.line > parent.end.line) {
        return false;
    }
    if (child.start.line === parent.start.line && child.start.character < parent.start.character) {
        return false;
    }
    if (child.end.line === parent.end.line && child.end.character > parent.end.character) {
        return false;
    }
    return true;
}
function isEqualRange(a, b) {
    return a.start.line === b.start.line
        && a.start.character === b.start.character
        && a.end.line === b.end.line
        && a.end.character === b.end.character;
}
function stringToSnapshot(str) {
    return {
        getText: (start, end) => str.substring(start, end),
        getLength: () => str.length,
        getChangeRange: () => undefined,
    };
}
function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}
//# sourceMappingURL=common.js.map