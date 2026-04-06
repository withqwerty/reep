/*---------------------------------------------------------------------------------------------
*  Copyright (c) Microsoft Corporation. All rights reserved.
*  Licensed under the MIT License. See License.txt in the project root for license information.
*--------------------------------------------------------------------------------------------*/

declare module '@emmetio/html-matcher' {
	import type { BufferStream, HtmlNode } from 'EmmetNode';
	import type { HtmlNode as HtmlFlatNode } from 'EmmetFlatNode';

	function parse(stream: BufferStream): HtmlNode;
	function parse(stream: string): HtmlFlatNode;

	export default parse;
}
