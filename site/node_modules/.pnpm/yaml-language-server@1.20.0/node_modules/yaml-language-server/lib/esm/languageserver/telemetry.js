/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Red Hat, Inc. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/
import { convertErrorToTelemetryMsg } from '../languageservice/utils/objects';
export class TelemetryImpl {
    constructor(connection) {
        this.connection = connection;
    }
    send(event) {
        this.connection.telemetry.logEvent(event);
    }
    sendError(name, error) {
        this.send({ name, type: 'track', properties: { error: convertErrorToTelemetryMsg(error) } });
    }
    sendTrack(name, properties) {
        this.send({ name, type: 'track', properties: properties });
    }
}
//# sourceMappingURL=telemetry.js.map