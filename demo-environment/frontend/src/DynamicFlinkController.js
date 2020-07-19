import React, { Component } from 'react'
import styled from 'styled-components'

import TopicLog from './TopicLog'
import RuleList from './RuleList'

const URL = 'ws://localhost:3002'

const StreamMonitor = styled.div`
  display: flex;
`

const Stream = styled.div`
  flex: 50%;
  background-color: #393d43;
  color: white;
`

class DynamicFlinkController extends Component {
  state = {
    events: [],
    alerts: [],
    rules:  [],
  }

  ws = new WebSocket(URL)

  componentDidMount() {
    this.ws.onopen = () => {
      console.log('connected')
    }

    this.ws.onmessage = evt => {
      var message = JSON.parse(evt.data);

      switch (message.type) {
        case 'events':
          this.setState(state => ({ events: [message.data, ...state.events] }))
          break;
        case 'alerts':
          this.setState(state => ({ alerts: [message.data, ...state.alerts] }))
          break;
          case 'control-output-stream':
            console.log(message.data)
            break;
          case 'rule-list':
            this.setState(() => ({ rules: message.data }))
            break;
  
        default:
          console.log(`Unexpected message type: ${message.type}`)
          break;
      }
    }

    this.ws.onclose = () => {
      console.log('disconnected')
      // automatically try to reconnect on connection loss
      this.setState({
        ws: new WebSocket(URL),
      })
    }
  }

  refreshRules() {
    var command = { request: "list-rules" }
    this.ws.send(JSON.stringify(command))
  }

  render() {
    return (
      <div>
        <RuleList 
          rules={this.state.rules}
          onClick={() => this.refreshRules()} />
        <StreamMonitor>
          <Stream><TopicLog topicName='Events' messages={this.state.events} /></Stream>
          <Stream><TopicLog topicName='Alerts' messages={this.state.alerts} /></Stream>
        </StreamMonitor>
      </div>
    )
  }
}

export default DynamicFlinkController