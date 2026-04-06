"use strict";
/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Red Hat, Inc. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/
Object.defineProperty(exports, "__esModule", { value: true });
exports.TelemetryImpl = void 0;
const objects_1 = require("../languageservice/utils/objects");
class TelemetryImpl {
    constructor(connection) {
        this.connection = connection;
    }
    send(event) {
        this.connection.telemetry.logEvent(event);
    }
    sendError(name, error) {
        this.send({ name, type: 'track', properties: { error: (0, objects_1.convertErrorToTelemetryMsg)(error) } });
    }
    sendTrack(name, properties) {
        this.send({ name, type: 'track', properties: properties });
    }
}
exports.TelemetryImpl = TelemetryImpl;
//# sourceMappingURL=telemetry.js.map