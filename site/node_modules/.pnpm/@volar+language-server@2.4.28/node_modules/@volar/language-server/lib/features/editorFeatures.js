"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.register = register;
const language_service_1 = require("@volar/language-service");
const vscode_uri_1 = require("vscode-uri");
const protocol_1 = require("../../protocol");
function register(server) {
    server.onInitialize(() => {
        const { project } = server;
        const scriptVersions = (0, language_service_1.createUriMap)();
        const scriptVersionSnapshots = new WeakSet();
        server.connection.onRequest(protocol_1.GetMatchTsConfigRequest.type, async (params) => {
            const uri = vscode_uri_1.URI.parse(params.uri);
            const languageService = await project.getLanguageService(uri);
            const tsProject = languageService.context.project.typescript;
            if (tsProject?.configFileName) {
                const { configFileName, uriConverter } = tsProject;
                return { uri: uriConverter.asUri(configFileName).toString() };
            }
        });
        server.connection.onRequest(protocol_1.GetVirtualFileRequest.type, async (document) => {
            const uri = vscode_uri_1.URI.parse(document.uri);
            const languageService = await project.getLanguageService(uri);
            const documentUri = vscode_uri_1.URI.parse(document.uri);
            const sourceScript = languageService.context.language.scripts.get(documentUri);
            if (sourceScript?.generated) {
                return prune(sourceScript.generated.root);
            }
            function prune(virtualCode) {
                const uri = languageService.context.encodeEmbeddedDocumentUri(sourceScript.id, virtualCode.id);
                let version = scriptVersions.get(uri) ?? 0;
                if (!scriptVersionSnapshots.has(virtualCode.snapshot)) {
                    version++;
                    scriptVersions.set(uri, version);
                    scriptVersionSnapshots.add(virtualCode.snapshot);
                }
                return {
                    fileUri: sourceScript.id.toString(),
                    virtualCodeId: virtualCode.id,
                    languageId: virtualCode.languageId,
                    embeddedCodes: virtualCode.embeddedCodes?.map(prune) || [],
                    version,
                    disabled: languageService.context.disabledEmbeddedDocumentUris.has(uri),
                };
            }
        });
        server.connection.onRequest(protocol_1.GetVirtualCodeRequest.type, async (params) => {
            const uri = vscode_uri_1.URI.parse(params.fileUri);
            const languageService = await project.getLanguageService(uri);
            const sourceScript = languageService.context.language.scripts.get(vscode_uri_1.URI.parse(params.fileUri));
            const virtualCode = sourceScript?.generated?.embeddedCodes.get(params.virtualCodeId);
            if (virtualCode) {
                const mappings = {};
                for (const [sourceScript, map] of languageService.context.language.maps.forEach(virtualCode)) {
                    mappings[sourceScript.id.toString()] = map.mappings;
                }
                return {
                    content: virtualCode.snapshot.getText(0, virtualCode.snapshot.getLength()),
                    mappings,
                };
            }
        });
        server.connection.onNotification(protocol_1.UpdateVirtualCodeStateNotification.type, async (params) => {
            const uri = vscode_uri_1.URI.parse(params.fileUri);
            const languageService = await project.getLanguageService(uri);
            const virtualFileUri = languageService.context.encodeEmbeddedDocumentUri(vscode_uri_1.URI.parse(params.fileUri), params.virtualCodeId);
            if (params.disabled) {
                languageService.context.disabledEmbeddedDocumentUris.set(virtualFileUri, true);
            }
            else {
                languageService.context.disabledEmbeddedDocumentUris.delete(virtualFileUri);
            }
        });
        server.connection.onNotification(protocol_1.UpdateServicePluginStateNotification.type, async (params) => {
            const uri = vscode_uri_1.URI.parse(params.uri);
            const languageService = await project.getLanguageService(uri);
            const plugin = languageService.context.plugins[params.serviceId][1];
            if (params.disabled) {
                languageService.context.disabledServicePlugins.add(plugin);
            }
            else {
                languageService.context.disabledServicePlugins.delete(plugin);
            }
        });
        server.connection.onRequest(protocol_1.GetServicePluginsRequest.type, async (params) => {
            const uri = vscode_uri_1.URI.parse(params.uri);
            const languageService = await project.getLanguageService(uri);
            const result = [];
            for (let pluginIndex = 0; pluginIndex < languageService.context.plugins.length; pluginIndex++) {
                const plugin = languageService.context.plugins[pluginIndex];
                result.push({
                    id: pluginIndex,
                    name: plugin[0].name,
                    disabled: languageService.context.disabledServicePlugins.has(plugin[1]),
                    features: Object.keys(plugin[1]),
                });
            }
            return result;
        });
    });
}
//# sourceMappingURL=editorFeatures.js.map