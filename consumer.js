var client = require('beanstalk_client').Client;
var request = require('request');

client.connect('127.0.0.1:11300', function(err, conn) {
    var reserve = function() {
        conn.reserve(function(err, job_id, job_json) {
            console.log('got job: ' + job_id);
            console.log('got job data: ' + job_json);
            //{"data":{"name":"node-beanstalk-client"}}
	    var text_to_parse = JSON.parse(job_json).data.name;
	   if(typeof text_to_parse != "undefined") {
    		findSentiment(text_to_parse, job_id);
	   } else {
	    //TODO: Destroy the undefined message from the queue.
	}
        });
    }
    reserve();
});

function findSentiment(text_to_parse, job_id){
	request.post(
	    'http://text-processing.com/api/sentiment/',
	    { form: { text: text_to_parse } },
	    function (error, response, body) {
		if (!error && response.statusCode == 200) {
		    console.log(body)
		    // remove the message from the queue
		    client.connect('127.0.0.1:11300', function(err, conn) {
			conn.destroy(job_id, function(err) {
				console.log("destroyed job" + job_id);
			});
		    });
		} else {
		    // TODO : increment the retry_attempt and put it in the queue using the producer method defined below
		    // can call a function and that can be asynchronous
		}
	    }
	);
}

function producer(data){
	var client = require('beanstalk_client').Client;
	client.connect('127.0.0.1:11300', function(err, conn) {
	    // data should be valid json
	    var job_data = {"data": data};
	    var priority = 0;
	    var delay = 5;
	    var timeToRun = 1;
	    conn.put(priority, delay, timeToRun, JSON.stringify(job_data), function(err, job_id) {
		console.log('put job: ' + job_id);
		//process.exit();
	    });
	});
}
