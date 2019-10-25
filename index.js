const amqp = require('amqplib/callback_api');
const queue = 'node_msg_queue';



//Code for connect to Msg queue
amqp.connect('amqp://localhost', (err, conn) => {
    try {
        if (err) {
            throw err;
        }
        //channel creation msg
        createChannel(conn);


    } catch (error) {
        console.log(`Error occurred ${error}`);
    }
});

const createChannel = (conn) => {
    try {

        conn.createChannel((err, channel) => {
            if (err) {
                console.log(err);
                throw err;
            }

            //sending msg
            //sendMsg(channel, 'Test msg1');

            //receiving msg
            receiveMsg(channel);




        });
    } catch (error) {
        console.log(`error ${error}`);
    }
    finally {
        setTimeout(() => {
            console.log(`closing connection`);
            conn.close();
            process.exit(0);
        }, 1500);
    }
}

const sendMsg = (channel, msg) => {
    try {

        channel.assertQueue(queue, { durable: true });

        channel.sendToQueue(queue, Buffer.from(msg), {
            persistent: true
        });

        console.log(`Msg ${msg} has been sent to queue ${queue}`);
    } catch (error) {
        console.log(`error ${error}`);
    }

}

const receiveMsg = (channel) => {
    try {
        channel.assertQueue(queue, { durable: true });

        channel.prefetch(1);

        console.log(`waiting for msg`);

        channel.consume(queue, (msg) => {
            console.log(`Msg received ${msg.content.toString()}`);
            setTimeout(function () {
                channel.ack(msg);
            }, 1000);
        });
        

    } catch (error) {
        console.log(`Error ${error}`);
    }
}