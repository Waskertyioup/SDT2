const express = require("express");
const cors = require("cors");
const { Kafka } = require('kafkajs')

const port = process.env.PORT || 3000;
const app = express();

app.use(cors());
app.use(express.json());

const kafka = new Kafka({
  brokers: [process.env.kafkaHost]
});

app.get("/", async (req, res) => {
  res.send("Hello World! Consumer");
});

app.listen(port, () => {
  console.log(`API RUN AT http://localhost:${port}`);
});

// the client ID lets kafka know who's producing the messages
const clientId = "consumer"
// we can define the list of brokers in the cluster
const brokers = ["0.0.0.0:9092"]
// this is the topic to which we want to write messages
const topic = "logs"


const consumer = kafka.consumer({ groupId: clientId })

usersBlocked = []

// Array que almacena los intentos de login
tries = []

// Funcion para leer los mensajes del topic
const consume = async () => {
	// first, we wait for the client to connect and subscribe to the given topic
	await consumer.connect()
	await consumer.subscribe({ topic })
	await consumer.run({
		// this function is called every time the consumer gets a new message
		eachMessage: ({ message }) => {




			// here, we just log the message to the standard output
			user = message.key
			date = message.value

			console.log(`Usuario: ${user}`)
			console.log(`Time: ${date}`)

			datalog = [user.toString(), date];

			tries.push(datalog);

			// Revisa los Tries, buscando Banear
			date_acct = date = Date.now();
			for (var i = 0; i < tries.length; i++){
				resta = parseInt(date_acct) - parseInt(tries[i][1]);
				console.log(`Delay: ${resta}`)
			}
			/*

			
			*/

			/*
			if (attempts >= 5) {
				if (!usersBlocked.includes(user.toString())) {
					usersBlocked.push(user.toString())
					console.log(user.toString() + " bloqueado")
				}
			}
			*/
		},
	})
}

// Lee los mensajes en topic
app.get("/cons", async (req, res) => {
  consume();
  res.send("consumiendo") 
});

app.get('/blocked', (req, res) => {
	  res.send(JSON.stringify({ "users-blocked": usersBlocked }));
});

app.get('/tries', (req, res) => {
	res.send(JSON.stringify({ "tries": tries }));
});