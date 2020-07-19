import React, { Component } from 'react'
import styled from 'styled-components'

const Button = styled.a`
  /* This renders the buttons above... Edit me! */
  display: inline-block;
  border-radius: 3px;
  padding: 0.5rem 0;
  margin: 0.5rem 1rem;
  width: 11rem;
  background: transparent;
  color: black;
  border: 2px solid black;
`

class RuleList extends Component {
  render() {
    return (
      <div>
        <h2>Rules</h2>
        <Button onClick={() => this.props.onClick()}>Refresh</Button>
        {this.props.rules.map((rule) =>
          <div>
            <div>{rule.id}</div>
            <div>{rule.version}</div>
            <div>{rule.content}</div>
          </div>
        )}
      </div>
    )
  }
}

export default RuleList