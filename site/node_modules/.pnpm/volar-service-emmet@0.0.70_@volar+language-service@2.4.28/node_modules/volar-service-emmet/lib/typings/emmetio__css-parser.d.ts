/*---------------------------------------------------------------------------------------------
*  Copyright (c) Microsoft Corporation. All rights reserved.
*  Licensed under the MIT License. See License.txt in the project root for license information.
*--------------------------------------------------------------------------------------------*/

declare module '@emmetio/css-parser' {
	import type { BufferStream, Stylesheet } from 'EmmetNode';
	import type { Stylesheet as FlatStylesheet } from 'EmmetFlatNode';

	function parseStylesheet(stream: BufferStream): Stylesheet;
	function parseStylesheet(stream: string): FlatStylesheet;

	export default parseStylesheet;
}
