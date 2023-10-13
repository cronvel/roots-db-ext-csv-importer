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

const EventEmitter = require( 'events' ) ;
const fs = require( 'fs' ) ;



function CsvImporter( params = {} ) {
	//extension.host.api.log.hdebug( "params %I" , params ) ;
	this.file = params.file ;

	// Format options
	this.separator = params.format?.separator || undefined ;
	this.quote = params.format?.quote || undefined ;
	this.newline = params.format?.newline || undefined ;
	this.escape = params.format?.escape || undefined ;

	this.propertyMapping = params.propertyMapping || {} ;
}

CsvImporter.prototype = Object.create( EventEmitter.prototype ) ;
CsvImporter.prototype.constructor = CsvImporter ;

module.exports = CsvImporter ;



CsvImporter.type = 'csv' ;



CsvImporter.prototype.import = function() {
	var stream = fs.createReadStream( this.file ) ;

	var options = {} ;

	if ( this.separator ) { options.separator = this.separator ; }
	if ( this.quote ) { options.quote = this.quote ; }
	if ( this.newline ) { options.newline = this.newline ; }
	if ( this.escape ) { options.escape = this.escape ; }

	options.mapHeaders = ( { header , index } ) => this.propertyMapping[ header ] || header ;
	options.mapHeaders = ( { header , index } ) => {
		extension.host.api.log.hdebug( "map header: %s -> %s" , header , this.propertyMapping[ header ] || header ) ;
		return this.propertyMapping[ header ] || header ;
	} ;

	options.mapValues = ( { header , index , value } ) => {
		return value ;
	} ;

	var parser = csvParser( options ) ;

	var rawDocumentStream = stream.pipe( parser ) ;
	rawDocumentStream.on( 'data' , rawDocument => this.emit( 'rawDocument' , rawDocument ) ) ;

	return new Promise( ( resolve , reject ) => {
		rawDocumentStream.on( 'end' , () => resolve() ) ;
	} ) ;
} ;

