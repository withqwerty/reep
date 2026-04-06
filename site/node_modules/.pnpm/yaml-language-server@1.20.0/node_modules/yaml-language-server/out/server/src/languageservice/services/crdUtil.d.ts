import { SingleYAMLDocument } from '../parser/yamlParser07';
import { JSONDocument } from '../parser/jsonDocument';
import { ResolvedSchema } from 'vscode-json-languageservice/lib/umd/services/jsonSchemaService';
/**
 * Retrieve schema by auto-detecting the Kubernetes GroupVersionKind (GVK) from the document.
 * If there is no definition for the GVK in the main kubernetes schema,
 * the schema is then retrieved from the CRD catalog.
 * Public for testing purpose, not part of the API.
 * @param doc
 * @param crdCatalogURI The URL of the CRD catalog to retrieve the schema from
 * @param kubernetesSchema The main kubernetes schema, if it includes a definition for the GVK it will be used
 */
export declare function autoDetectKubernetesSchemaFromDocument(doc: SingleYAMLDocument | JSONDocument, crdCatalogURI: string, kubernetesSchema: ResolvedSchema): string | undefined;
/**
 * Retrieve the group, version and kind from the document.
 * Public for testing purpose, not part of the API.
 * @param doc
 */
export declare function getGroupVersionKindFromDocument(doc: SingleYAMLDocument | JSONDocument): {
    group: string;
    version: string;
    kind: string;
} | undefined;
