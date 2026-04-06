import { InitializeParams } from 'vscode-languageserver';
/**
 * Loads translations from the filesystem based on the configured locale and the folder of translations provided in the initialization parameters.
 *
 * This is the default implementation when running as binary, but isn't used when running as a web worker.
 *
 * @param params the language server initialization parameters
 */
export declare function setupl10nBundle(params: InitializeParams): Promise<void>;
