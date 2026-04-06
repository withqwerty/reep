(function (factory) {
    if (typeof module === "object" && typeof module.exports === "object") {
        var v = factory(require, exports);
        if (v !== undefined) module.exports = v;
    }
    else if (typeof define === "function" && define.amd) {
        define(["require", "exports", "../parser/yamlParser07"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    exports.getGroupVersionKindFromDocument = exports.autoDetectKubernetesSchemaFromDocument = void 0;
    const yamlParser07_1 = require("../parser/yamlParser07");
    /**
     * Retrieve schema by auto-detecting the Kubernetes GroupVersionKind (GVK) from the document.
     * If there is no definition for the GVK in the main kubernetes schema,
     * the schema is then retrieved from the CRD catalog.
     * Public for testing purpose, not part of the API.
     * @param doc
     * @param crdCatalogURI The URL of the CRD catalog to retrieve the schema from
     * @param kubernetesSchema The main kubernetes schema, if it includes a definition for the GVK it will be used
     */
    function autoDetectKubernetesSchemaFromDocument(doc, crdCatalogURI, kubernetesSchema) {
        const res = getGroupVersionKindFromDocument(doc);
        if (!res) {
            return undefined;
        }
        const { group, version, kind } = res;
        if (!group || !version || !kind) {
            return undefined;
        }
        const k8sSchema = kubernetesSchema.schema;
        const kubernetesBuildIns = (k8sSchema.oneOf || [])
            .map((s) => {
            if (typeof s === 'boolean') {
                return undefined;
            }
            return s._$ref || s.$ref;
        })
            .filter((ref) => ref)
            .map((ref) => ref.replace('_definitions.json#/definitions/', '').toLowerCase());
        const groupWithoutK8sIO = group.replace('.k8s.io', '');
        const k8sTypeName = `io.k8s.api.${groupWithoutK8sIO.toLowerCase()}.${version.toLowerCase()}.${kind.toLowerCase()}`;
        if (kubernetesBuildIns.includes(k8sTypeName)) {
            return undefined;
        }
        if (k8sTypeName.includes('openshift.io')) {
            return `${crdCatalogURI}/openshift/v4.15-strict/${kind.toLowerCase()}_${group.toLowerCase()}_${version.toLowerCase()}.json`;
        }
        const schemaURL = `${crdCatalogURI}/${group.toLowerCase()}/${kind.toLowerCase()}_${version.toLowerCase()}.json`;
        return schemaURL;
    }
    exports.autoDetectKubernetesSchemaFromDocument = autoDetectKubernetesSchemaFromDocument;
    /**
     * Retrieve the group, version and kind from the document.
     * Public for testing purpose, not part of the API.
     * @param doc
     */
    function getGroupVersionKindFromDocument(doc) {
        if (doc instanceof yamlParser07_1.SingleYAMLDocument) {
            try {
                const rootJSON = doc.root.internalNode.toJSON();
                if (!rootJSON) {
                    return undefined;
                }
                const groupVersion = rootJSON['apiVersion'];
                if (!groupVersion) {
                    return undefined;
                }
                const [group, version] = groupVersion.split('/');
                if (!group || !version) {
                    return undefined;
                }
                const kind = rootJSON['kind'];
                if (!kind) {
                    return undefined;
                }
                return { group, version, kind };
            }
            catch (error) {
                console.error('Error parsing YAML document:', error);
                return undefined;
            }
        }
        return undefined;
    }
    exports.getGroupVersionKindFromDocument = getGroupVersionKindFromDocument;
});
//# sourceMappingURL=crdUtil.js.map