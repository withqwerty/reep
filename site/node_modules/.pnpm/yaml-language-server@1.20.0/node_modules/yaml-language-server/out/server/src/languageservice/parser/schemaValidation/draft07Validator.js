"use strict";
/*---------------------------------------------------------------------------------------------
 *  Copyright (c) IBM Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/
Object.defineProperty(exports, "__esModule", { value: true });
exports.Draft07Validator = void 0;
const jsonSchema_1 = require("../../jsonSchema");
const objects_1 = require("../../utils/objects");
const baseValidator_1 = require("./baseValidator");
class Draft07Validator extends baseValidator_1.BaseValidator {
    getCurrentDialect() {
        return jsonSchema_1.SchemaDialect.draft07;
    }
    /**
     * Keyword: exclusiveMinimum/exclusiveMaximum are treated as numeric bounds
     */
    getNumberLimits(schema) {
        const minimum = (0, objects_1.isNumber)(schema.minimum) ? schema.minimum : undefined;
        const maximum = (0, objects_1.isNumber)(schema.maximum) ? schema.maximum : undefined;
        const exclusiveMinimum = (0, objects_1.isNumber)(schema.exclusiveMinimum) ? schema.exclusiveMinimum : undefined;
        const exclusiveMaximum = (0, objects_1.isNumber)(schema.exclusiveMaximum) ? schema.exclusiveMaximum : undefined;
        return {
            minimum: exclusiveMinimum === undefined ? minimum : undefined,
            maximum: exclusiveMaximum === undefined ? maximum : undefined,
            exclusiveMinimum,
            exclusiveMaximum,
        };
    }
}
exports.Draft07Validator = Draft07Validator;
//# sourceMappingURL=draft07Validator.js.map