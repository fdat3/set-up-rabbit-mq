import { io } from 'socket.io-client'

// establish socket connection and run server on port 4000
const socket = io('http://localhost:3000')

// bring in the container, input field where message is entered and send button
const chat = document.querySelector('.container')
const input = document.querySelector('#message')
const send = document.querySelector('#btn-send')

// function appends the entered message in the container on submit
const sendMessage = (message) => {
    const text = document.createElement('p')
    text.innerText = message
    chat.appendChild(text)
}

// listen to send button click
send.addEventListener('click', (e) => {
    e.preventDefault()
    // if there is a valid message entered
    if (input.value) {
        sendMessage(`Sending: ${input.value}`)
        // emit the message to the server
        socket.emit("PUBLISH", input.value)
    }
    input.value = ''
})

// append connection ID to container on success
socket.on("connect", () => {
    sendMessage(`Connected with ID: ${socket.id}`)
})

// append message received from server to container
socket.on("SUBSCRIBE", (message) => {
    sendMessage(message)
})