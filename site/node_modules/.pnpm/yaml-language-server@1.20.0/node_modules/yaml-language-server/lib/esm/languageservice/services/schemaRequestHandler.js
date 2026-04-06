import { join } from 'path';
import { getErrorStatusDescription, xhr } from 'request-light';
import * as URL from 'url';
import { RequestType } from 'vscode-languageserver';
import { URI } from 'vscode-uri';
import { CustomSchemaContentRequest, VSCodeContentRequest } from '../../requestTypes';
import { isRelativePath, relativeToAbsolutePath } from '../utils/paths';
// eslint-disable-next-line @typescript-eslint/no-namespace
var FSReadUri;
(function (FSReadUri) {
    FSReadUri.type = new RequestType('fs/readUri');
})(FSReadUri || (FSReadUri = {}));
/**
 * Handles schema content requests given the schema URI
 * @param uri can be a local file, vscode request, http(s) request or a custom request
 */
export const schemaRequestHandler = async (connection, uri, workspaceFolders, workspaceRoot, useVSCodeContentRequest, fs, isWeb) => {
    if (!uri) {
        return Promise.reject('No schema specified');
    }
    // If the requested schema URI is a relative file path
    // Convert it into a proper absolute path URI
    if (isRelativePath(uri)) {
        // HACK: the fs/readUri extension is only available with vscode-yaml,
        // and this fix is specific to vscode-yaml on web, so don't use it in other cases
        if (workspaceFolders.length === 1 && isWeb) {
            const wsUri = URI.parse(workspaceFolders[0].uri);
            const wsDirname = wsUri.path;
            const modifiedUri = wsUri.with({ path: join(wsDirname, uri) });
            try {
                return connection.sendRequest(FSReadUri.type, modifiedUri.toString());
            }
            catch (e) {
                connection.window.showErrorMessage(`failed to get content of '${modifiedUri}': ${e}`);
            }
        }
        else {
            uri = relativeToAbsolutePath(workspaceFolders, workspaceRoot, uri);
        }
    }
    let scheme = URI.parse(uri).scheme.toLowerCase();
    // test if uri is windows path, ie starts with 'c:\'
    if (/^[a-z]:[\\/]/i.test(uri)) {
        const winUri = URI.file(uri);
        scheme = winUri.scheme.toLowerCase();
        uri = winUri.toString();
    }
    // If the requested schema is a local file, read and return the file contents
    if (scheme === 'file') {
        const fsPath = URI.parse(uri).fsPath;
        return fs.readFile(fsPath, 'UTF-8').catch(() => {
            // If there was an error reading the file, return empty error message
            // Otherwise return the file contents as a string
            return '';
        });
    }
    // HTTP(S) requests are sent and the response result is either the schema content or an error
    if (scheme === 'http' || scheme === 'https') {
        // If we are running inside of VSCode we need to make a content request. This content request
        // will make it so that schemas behind VPN's will resolve correctly
        if (useVSCodeContentRequest) {
            return connection.sendRequest(VSCodeContentRequest.type, uri).then((responseText) => {
                return responseText;
            }, (error) => {
                return Promise.reject(error.message);
            });
        }
        // Send the HTTP(S) schema content request and return the result
        const headers = { 'Accept-Encoding': 'gzip, deflate' };
        return xhr({ url: uri, followRedirects: 5, headers }).then((response) => {
            return response.responseText;
        }, (error) => {
            return Promise.reject(error.responseText || getErrorStatusDescription(error.status) || error.toString());
        });
    }
    // Neither local file nor vscode, nor HTTP(S) schema request, so send it off as a custom request
    return connection.sendRequest(CustomSchemaContentRequest.type, uri);
};
export const workspaceContext = {
    resolveRelativePath: (relativePath, resource) => {
        return URL.resolve(resource, relativePath);
    },
};
//# sourceMappingURL=schemaRequestHandler.js.map