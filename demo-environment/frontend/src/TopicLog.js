import React, { Component } from 'react'

class TopicLog extends Component {
  render() {
    return (
      <div>
        <h2>{this.props.topicName}</h2>
        {this.props.messages.map((message) =>
          <p>{message}</p>
        )}
      </div>
    )
  }
}

export default TopicLog