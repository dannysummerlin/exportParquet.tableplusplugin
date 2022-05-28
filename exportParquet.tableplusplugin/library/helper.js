"use strict";

var parquet = require('parquetjs');

function dumpTableAsDefinition(context, item) {
	context.itemDefinition(item, function(creation) {
		SystemService.insertToClipboard(creation);
		SystemService.notify("Copy creation", item.type() + " " + item.name() + " creation statement is copied!");
	});
}

function camelize(str) {
	return str.replace(/(?:^\w|[A-Z]|\b\w|_\w)/g, function(letter, index) { return letter.toUpperCase(); }).replace(/\s+|-|_/g, "");
}

function getColumnFormat(columnName, dataType) {
	let typeArr = dataType.split("(");
	let typeOnly = typeArr[0];
	const types = {
		'varchar': 'UTF8',
		'float': 'FLOAT',
		'double': 'DOUBLE',
		'decimal': 'DOUBLE',
		'float4': 'FLOAT',
		'float8': 'FLOAT',
		'char': 'UTF8',
		'enum': 'UTF8',
		'int8': 'INT_64',
		'bigint': 'INT_64',
		'int': 'INT_8',
		'int4': 'INT_8',
		'int3': 'INT_16',
		'mediumint': 'INT_16',
		'int2': 'INT_8',
		'smallint': 'INT_8',
		'int1': 'INT_8',
		'tinyint': 'INT_8'
	}
	let returnType = types[typeOnly];
	if (dataType.includes("unsigned")) {
		returnType = 'U' + returnType;
	}
	return returnType;
}

function transformToParquet(context, item) {
	const schema = createSchema(context, item);
	// filepath - try to do file selection popup
	let filePath = 'temp.parquet';
	let writer = await parquet.ParquetWriter.openFile(schema, filePath);
	appendData(context,item, writer);
	write.close();
}

function appendData(context,item, writer) {
	const query = `SELECT * FROM '${item.name()}';`;
	context.execute(query, res => {
		res.rows.forEach(row => {
			await writer.appendRow(JSON.encode(row));
		});
	});
}

function createSchema(context, item) {
	let nameCamelcase = camelize(item.name());
	let schemaObject = {};
	let query;
	let driver = context.driver();
	switch (driver) {
		case "MySQL":
		case "MariaDB":
			query = `SELECT ordinal_position as ordinal_position,column_name as column_name,column_type AS data_type,column_name AS foreign_key FROM information_schema.columns WHERE table_schema='${item.schema()}'AND table_name='${item.name()}';`;
			break;
		case "PostgreSQL":
			query = `SELECT ordinal_position,column_name,udt_name AS data_type,numeric_precision,datetime_precision,numeric_scale,character_maximum_length AS data_length,column_name as check,column_name as check_constraint,column_default,column_name AS foreign_key FROM information_schema.columns WHERE table_name='${item.name()}'AND table_schema='${item.schema()}';`;
			break;
		default:
			context.alert("Error", driver + " is not supported");
			return;
	}
	context.execute(query, res => {
		res.rows.sort((l, r) => {
			return (
				parseInt(l.raw("ordinal_position")) >
				parseInt(r.raw("ordinal_position"))
			);
		});
		res.rows.forEach(row => {
			let columnName = row.raw("column_name");
			let columnType = row.raw("data_type");
			schemaObject[columnName] = { type: getColumnFormat(columnName, columnType) };
		});

		return (new parquet.ParquetSchema(schemaObject));
	});
}

export { transformToParquet };
