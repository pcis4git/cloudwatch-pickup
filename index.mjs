import * as zlib from 'node:zlib';
import { Kafka } from 'kafkajs';

export const handler = async (event) => {

    const payload = Buffer.from(event.awslogs.data, 'base64');
    const parsed = JSON.parse(zlib.gunzipSync(payload).toString('utf8'));
    const logEvents = parsed.logEvents;
    
    var messages4Topic = [];
    logEvents.forEach( ( logEvent, index) => {
        const msg = logEvent['message'];
        const msgJson = JSON.parse( msg );
        const containerName = msgJson['kubernetes']['container_name'];
        
        if( 'backbone' == containerName ){
            delete msgJson['kubernetes']
            msgJson['containerName'] = containerName;

            console.log('identified one log entr for KAFKA');
            const record = { "value" : JSON.stringify( msgJson ) }            
            messages4Topic.push( record );
        }        
    } );

    if( messages4Topic.length == 0 ){
        console.log('No log entry for KAFKA');
    }
    else {
        const kafkaReq = {
            topic: 'oag-log-topic',
            messages: messages4Topic
        };
        console.log('KAFKA Request: ', JSON.stringify(kafkaReq));

        const kafka = new Kafka({
            clientId: 'lambda-producer',
            brokers: ['b-1.oagkafka.88qaet.c2.kafka.ca-central-1.amazonaws.com:9092','b-2.oagkafka.88qaet.c2.kafka.ca-central-1.amazonaws.com:9092'],
            ssl: false
        });      
        
        let producer = null;
        try {
            producer = kafka.producer();
            await producer.connect();
            await producer.send(kafkaReq);
        }
        catch (e) {
            console.log('KafkaJS Error: ', e);
        }
        finally {
            await producer.disconnect();
        }

    }

    return 'Success';
};