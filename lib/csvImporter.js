/*
	Roots DB Fake Data Generation

	Copyright (c) 2023 Cédric Ronvel

	The MIT License (MIT)

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
*/

"use strict" ;



const extension = require( './extension.js' ) ;
const csvParser = require( 'csv-parser' ) ;



function CsvImporter( stream ) {
	this.stream = stream ;
	extension.host.api.log.hdebug( "Plop!" ) ;
}

module.exports = CsvImporter ;



CsvImporter.prototype.import = function() {
	extension.host.api.log.hdebug( "Plop!2" ) ;
	this.stream.pipe( csvParser() )
		.on( 'data' , data => {
			extension.host.api.log.hdebug( "Received row: %I" , data ) ;
		} )
		.on( 'end' , () => {
			extension.host.api.log.hdebug( "End!" ) ;
		} ) ;
} ;

