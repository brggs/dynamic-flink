import React, { Component } from 'react'
import styled from 'styled-components'
import RuleView from './RuleView'

const Button = styled.a`
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
        <Button onClick={() => this.props.onRefreshClick()}>Refresh</Button>
        <Button onClick={() => this.props.onAddClick()}>Add...</Button>
        {Object.entries(this.props.rules).map((t, k) =>
          <RuleView
            rule={t[1]}
            onContentChange={(content) => this.props.onUpdateRuleContent(k, content)}
            onUpdateClick={rule => this.props.onUpdateClick(rule)}
            onRemoveClick={rule => this.props.onRemoveClick(rule)} />
        )}
      </div>
    )
  }
}

export default RuleList