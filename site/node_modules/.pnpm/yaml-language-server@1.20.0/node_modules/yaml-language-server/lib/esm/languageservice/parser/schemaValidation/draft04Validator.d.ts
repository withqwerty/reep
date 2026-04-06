import type { JSONSchema } from '../../jsonSchema';
import { SchemaDialect } from '../../jsonSchema';
import { BaseValidator } from './baseValidator';
export declare class Draft04Validator extends BaseValidator {
    protected getCurrentDialect(): SchemaDialect;
    /**
     * Keyword: exclusiveMinimum/exclusiveMaximum
     *
     * Booleans that make minimum/maximum exclusive.
     */
    protected getNumberLimits(schema: JSONSchema): {
        minimum?: number;
        maximum?: number;
        exclusiveMinimum?: number;
        exclusiveMaximum?: number;
    };
}
