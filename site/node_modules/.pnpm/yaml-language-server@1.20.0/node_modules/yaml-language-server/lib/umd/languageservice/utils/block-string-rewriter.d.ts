import { Scalar } from 'yaml';
export declare class BlockStringRewriter {
    private readonly indentation;
    private readonly maxLineLength;
    constructor(indentation: string, maxLineLength: number);
    writeFoldedBlockScalar(node: Scalar<string>): string | null;
    writeLiteralBlockScalar(node: Scalar<string>): string | null;
}
