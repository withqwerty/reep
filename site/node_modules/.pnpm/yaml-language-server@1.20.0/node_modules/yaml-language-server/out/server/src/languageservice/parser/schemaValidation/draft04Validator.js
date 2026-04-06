"use strict";
/*---------------------------------------------------------------------------------------------
 *  Copyright (c) IBM Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/
Object.defineProperty(exports, "__esModule", { value: true });
exports.Draft04Validator = void 0;
const jsonSchema_1 = require("../../jsonSchema");
const objects_1 = require("../../utils/objects");
const baseValidator_1 = require("./baseValidator");
class Draft04Validator extends baseValidator_1.BaseValidator {
    getCurrentDialect() {
        return jsonSchema_1.SchemaDialect.draft04;
    }
    /**
     * Keyword: exclusiveMinimum/exclusiveMaximum
     *
     * Booleans that make minimum/maximum exclusive.
     */
    getNumberLimits(schema) {
        const minimum = (0, objects_1.isNumber)(schema.minimum) ? schema.minimum : undefined;
        const maximum = (0, objects_1.isNumber)(schema.maximum) ? schema.maximum : undefined;
        const exclusiveMinimum = (0, objects_1.isBoolean)(schema.exclusiveMinimum) && schema.exclusiveMinimum ? minimum : undefined;
        const exclusiveMaximum = (0, objects_1.isBoolean)(schema.exclusiveMaximum) && schema.exclusiveMaximum ? maximum : undefined;
        return {
            minimum: exclusiveMinimum === undefined ? minimum : undefined,
            maximum: exclusiveMaximum === undefined ? maximum : undefined,
            exclusiveMinimum,
            exclusiveMaximum,
        };
    }
}
exports.Draft04Validator = Draft04Validator;
//# sourceMappingURL=draft04Validator.js.map