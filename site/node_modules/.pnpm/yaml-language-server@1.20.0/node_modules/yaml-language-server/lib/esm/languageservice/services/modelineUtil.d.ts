import { SingleYAMLDocument } from '../parser/yamlParser07';
import { JSONDocument } from '../parser/jsonDocument';
/**
 * Retrieve schema if declared as modeline.
 * Public for testing purpose, not part of the API.
 * @param doc
 */
export declare function getSchemaFromModeline(doc: SingleYAMLDocument | JSONDocument): string;
export declare function isModeline(lineText: string): boolean;
