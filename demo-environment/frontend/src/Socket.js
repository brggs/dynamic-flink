import React, { Component } from 'react'

const URL = 'ws://localhost:3002'

class Socket extends Component {
  state = {
    messages: [],
  }

  ws = new WebSocket(URL)

  componentDidMount() {
    this.ws.onopen = () => {
      // on connecting, do nothing but log it to the console
      console.log('connected')
    }

    this.ws.onmessage = evt => {
      console.log(evt.data)
      this.addMessage(evt.data)
    }

    this.ws.onclose = () => {
      console.log('disconnected')
      // automatically try to reconnect on connection loss
      this.setState({
        ws: new WebSocket(URL),
      })
    }
  }

  addMessage = message =>
    this.setState(state => ({ messages: [message, ...state.messages] }))

  render() {
    return (
      <div>
        <p>Test</p>
        {this.state.messages.map((message) =>
          <p>{message}</p>
        )}
      </div>
    )
  }
}

export default Socket