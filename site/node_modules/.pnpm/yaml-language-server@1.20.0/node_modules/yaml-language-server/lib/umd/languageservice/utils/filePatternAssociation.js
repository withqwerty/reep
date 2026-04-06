(function (factory) {
    if (typeof module === "object" && typeof module.exports === "object") {
        var v = factory(require, exports);
        if (v !== undefined) module.exports = v;
    }
    else if (typeof define === "function" && define.amd) {
        define(["require", "exports", "./strings"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    exports.FilePatternAssociation = void 0;
    const strings_1 = require("./strings");
    class FilePatternAssociation {
        constructor(pattern) {
            try {
                this.patternRegExp = new RegExp((0, strings_1.convertSimple2RegExpPattern)(pattern) + '$');
            }
            catch (e) {
                // invalid pattern
                this.patternRegExp = null;
            }
            this.schemas = [];
        }
        addSchema(id) {
            this.schemas.push(id);
        }
        matchesPattern(fileName) {
            return this.patternRegExp && this.patternRegExp.test(fileName);
        }
        getSchemas() {
            return this.schemas;
        }
    }
    exports.FilePatternAssociation = FilePatternAssociation;
});
//# sourceMappingURL=filePatternAssociation.js.map