'use strict';

import { transformToParquet } from './library/helper';

const parquet = function(context)  {
	let item = context.clickedItem();
	if (item == null) {
		context.alert('Error', 'Please select a Table');
		return;
	}
	transformToParquet(context, item);
}

global.parquet = parquet;
