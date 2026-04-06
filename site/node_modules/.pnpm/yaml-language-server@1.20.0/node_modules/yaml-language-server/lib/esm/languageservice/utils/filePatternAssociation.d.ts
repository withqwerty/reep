export declare class FilePatternAssociation {
    private schemas;
    private patternRegExp;
    constructor(pattern: string);
    addSchema(id: string): void;
    matchesPattern(fileName: string): boolean;
    getSchemas(): string[];
}
