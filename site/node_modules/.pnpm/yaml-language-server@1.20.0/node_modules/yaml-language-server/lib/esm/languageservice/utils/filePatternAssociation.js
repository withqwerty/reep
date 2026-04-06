import { convertSimple2RegExpPattern } from './strings';
export class FilePatternAssociation {
    constructor(pattern) {
        try {
            this.patternRegExp = new RegExp(convertSimple2RegExpPattern(pattern) + '$');
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
//# sourceMappingURL=filePatternAssociation.js.map