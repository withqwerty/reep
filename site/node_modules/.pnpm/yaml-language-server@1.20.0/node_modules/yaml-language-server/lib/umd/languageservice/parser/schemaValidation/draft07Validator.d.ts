import type { JSONSchema } from '../../jsonSchema';
import { SchemaDialect } from '../../jsonSchema';
import { BaseValidator } from './baseValidator';
export declare class Draft07Validator extends BaseValidator {
    protected getCurrentDialect(): SchemaDialect;
    /**
     * Keyword: exclusiveMinimum/exclusiveMaximum are treated as numeric bounds
     */
    protected getNumberLimits(schema: JSONSchema): {
        minimum?: number;
        maximum?: number;
        exclusiveMinimum?: number;
        exclusiveMaximum?: number;
    };
}
