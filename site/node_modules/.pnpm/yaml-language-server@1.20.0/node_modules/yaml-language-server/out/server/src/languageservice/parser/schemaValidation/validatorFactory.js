"use strict";
/*---------------------------------------------------------------------------------------------
 *  Copyright (c) IBM Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/
Object.defineProperty(exports, "__esModule", { value: true });
exports.getValidator = void 0;
const jsonSchema_1 = require("../../jsonSchema");
const draft04Validator_1 = require("./draft04Validator");
const draft07Validator_1 = require("./draft07Validator");
const draft2019Validator_1 = require("./draft2019Validator");
const draft2020Validator_1 = require("./draft2020Validator");
function getValidator(dialect) {
    switch (dialect) {
        case jsonSchema_1.SchemaDialect.draft04:
            return new draft04Validator_1.Draft04Validator();
        case jsonSchema_1.SchemaDialect.draft07:
            return new draft07Validator_1.Draft07Validator();
        case jsonSchema_1.SchemaDialect.draft2019:
            return new draft2019Validator_1.Draft2019Validator();
        case jsonSchema_1.SchemaDialect.draft2020:
            return new draft2020Validator_1.Draft2020Validator();
        default:
            return new draft07Validator_1.Draft07Validator(); // fallback
    }
}
exports.getValidator = getValidator;
//# sourceMappingURL=validatorFactory.js.map