import { Kafka, Partitioners } from "kafkajs";
import { v4 as UUID } from "uuid";
console.log("*** Consumer starts... ***");

const kafka = new Kafka({
    clientId: 'checker-server',
    brokers: ['localhost:9092']
});
const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner })
const consumer = kafka.consumer({ groupId: 'kafka-checker-servers1' });

//check sent id
function checkId(value) {
    if (value.length == 11 && Number(value.substring(0, 1))) {
        return true;
    } else {
        return false;
    }
}

const run = async () => {
    //Consumer
    await consumer.connect()
    await consumer.subscribe({ topic: 'tobechecked', fromBeginning: true })

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            if (checkId(message.value.toString()) == true) {

                const run = async () => {
                    // Producing
                    await producer.connect()

                    setInterval(() => {
                        queueIDMessage(message.value);
                    }, 2500)
                }
                run().catch(console.error);

                console.log({
                    key: message.key.toString(),
                    partition: message.partition,
                    offset: message.offset,
                    value: message.value.toString(),
                })

            }
        },

    })
}

run().catch(console.error);

async function queueIDMessage(value) {

    const uuidFraction = UUID().substring(0, 4);
    // Producer
    const success = await producer.send({
        topic: 'checkedresults',
        messages: [
            {
                key: uuidFraction,
                value: value + ' is checked',
            },
        ],
    }
    );

    if (success) {
        console.log(`Checked result message ${uuidFraction} succesfully to the stream`);
    } else {
        console.log('Problem writing to the stream...')
    }
}