import { Diagnostic, DiagnosticSeverity, Range } from 'vscode-languageserver-types';
import { isMap, isSeq, visit } from 'yaml';
import * as l10n from '@vscode/l10n';
export class YAMLStyleValidator {
    constructor(settings) {
        this.forbidMapping = settings.flowMapping === 'forbid';
        this.forbidSequence = settings.flowSequence === 'forbid';
    }
    validate(document, yamlDoc) {
        const result = [];
        visit(yamlDoc.internalDocument, (key, node) => {
            if (this.forbidMapping && isMap(node) && node.srcToken?.type === 'flow-collection') {
                result.push(Diagnostic.create(this.getRangeOf(document, node.srcToken), l10n.t('Flow style mapping is forbidden'), DiagnosticSeverity.Error, 'flowMap'));
            }
            if (this.forbidSequence && isSeq(node) && node.srcToken?.type === 'flow-collection') {
                result.push(Diagnostic.create(this.getRangeOf(document, node.srcToken), l10n.t('Flow style sequence is forbidden'), DiagnosticSeverity.Error, 'flowSeq'));
            }
        });
        return result;
    }
    getRangeOf(document, node) {
        const endOffset = node.end[0].offset;
        let endPosition = document.positionAt(endOffset);
        endPosition = { character: endPosition.character + 1, line: endPosition.line };
        return Range.create(document.positionAt(node.start.offset), endPosition);
    }
}
//# sourceMappingURL=yaml-style.js.map