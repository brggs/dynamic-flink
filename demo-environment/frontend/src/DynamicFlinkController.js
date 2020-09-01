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
    rules: [],
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
          const data = JSON.parse(message.data)
          switch (data.status) {
            case 'STATUS_UPDATE':
              const newRules = JSON.parse(data.content).map(rule => {
                var temp = Object.assign({}, rule)
                temp.content = JSON.parse(rule.content)
                return temp
              });

              this.setState(() => ({ rules: newRules }))
              break;
            case 'RULE_ACTIVE':
            case 'RULE_DEACTIVATED':
              this.refreshRules()
              break;
            default:
              console.log(message.data)
              break;
          }
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

  addRule() {
    const nextRuleId = 1 + this.state.rules.reduce((prev, current) => (prev > parseInt(current.ruleId)) ? prev : parseInt(current.ruleId), 0)
    var command = {
      request: "add-rule",
      ruleId: nextRuleId,
      ruleVersion: "1",
      ruleContent: '{"id":null,"version":0,"baseSeverity":0,"blocks":[{"type":"SINGLE_EVENT","condition":{"@class":"uk.co.brggs.dynamicflink.blocks.conditions.EqualCondition","key":"colour","value":"Blue"},"windowSize":0,"windowSlide":0,"aggregationGroupingFields":null,"parameters":null}],"ruleCondition":null,"groupByField":null,"windowSize":0,"windowSlide":0}'
    }
    this.ws.send(JSON.stringify(command))
  }

  updateRule(rule) {
    var content = rule.content
    var newVersion = rule.ruleVersion + 1
    content.version = newVersion

    var command = {
      request: "update-rule",
      ruleId: rule.ruleId,
      ruleVersion: newVersion,
      ruleContent: JSON.stringify(content)
    }
    this.ws.send(JSON.stringify(command))
  }

  removeRule(rule) {
    var command = { request: "remove-rule", ruleId: rule.ruleId, ruleVersion: rule.ruleVersion }
    this.ws.send(JSON.stringify(command))
  }

  updateRuleContent = (i, content) => {
    this.setState(state => {
      const list = state.rules.map((item, j) => {
        if (j === i) {
          return { ...item, content: JSON.parse(content) };
        } else {
          return item;
        }
      });

      return {
        rules: list,
      };
    });
  };

  render() {
    return (
      <div>
        <RuleList
          rules={this.state.rules}
          onRefreshClick={() => this.refreshRules()}
          onAddClick={() => this.addRule()}
          onUpdateRuleContent={(i, content) => this.updateRuleContent(i, content)}
          onUpdateClick={rule => this.updateRule(rule)}
          onRemoveClick={rule => this.removeRule(rule)} />
        <StreamMonitor>
          <Stream><TopicLog topicName='Events' messages={this.state.events} /></Stream>
          <Stream><TopicLog topicName='Alerts' messages={this.state.alerts} /></Stream>
        </StreamMonitor>
      </div>
    )
  }
}

export default DynamicFlinkController