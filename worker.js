var Gearman =  require('node-gearman');
	MySQL =  require('mysql'),
	csv = require('csv'),
	util = require('util'),
	fs = require('fs'),
	config = require("./config.json"),
	client =  new Gearman(config.gearman.hostname, config.gearman.port),
	pool  = MySQL.createPool({
	  host     : config.mysql.hostname,
	  user     : config.mysql.user,
	  password : config.mysql.password,
	  database : config.mysql.database
	});

client.connect();
client.registerWorker("process-csv", function(payload, worker){
	if(!payload){
        worker.error();
        return;
    }

    fs.exists(payload.toString(), function(exists){
    	if(!exists){
    		worker.error("Path does not exist");
    		return;
    	}

    	var columns = [],
    		dataChunk = [],
    		chunkRead = 0;
    		chunkLimit = config.chunk_size;

		csv()
		.from.stream(fs.createReadStream(payload.toString()))
		.on('record', function(row, index){
			if(index == 0){
				//insertSQL 
				columns = row;
			}
		  	if(chunkRead < chunkLimit){
				dataChunk.push(row);
				chunkRead++;
		  	}else{
		  		insertData(config.table, columns, dataChunk);
		  		chunkRead = 0;
		  		dataChunk = [];
		  	}

		})
		.on('end', function(count){
			util.print("\nread "+count+" lines\n");
			worker.end("OK");
		})
		.on('error', function(error){
		  console.log(error.message);
		});
    });
});

var insertData = function (table, columns, data){
	pool.getConnection(function(err, connection){
		if(err){
			util.print(util.inspect(err));
			return false;
		}
		// util.print(util.inspect(connection, { showHidden: true, depth: 2 }));
		// process.exit();
		var query = connection.query("INSERT INTO ?? (??) VALUES ? ", [table, columns, data]);
		util.print(".");
		// util.print(util.inspect(query, { showHidden: true, depth: null }));

		query.on('error', function(err){
			util.print(util.inspect(err));
			util.print("Error writing");
			connection.release();
			process.exit();
		})
		.on('end', function(){
			// util.print(data.length+" written \n");
			connection.release();
		});
	});
}