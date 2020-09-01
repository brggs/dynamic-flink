import React, { Component } from 'react'
import styled from 'styled-components'
import Collapsible from './Collapsible'

const RuleInput = styled.textarea`
  width: 600px;
  height: 400px;
  display: block;
`

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

class RuleView extends Component {
  constructor(props) {
    super(props);
    this.handleChange = this.handleChange.bind(this);
  }

  handleChange(event) {
    this.props.onContentChange(event.target.value);
  }

  render() {
    return (
      <Collapsible title={`Rule: ${this.props.rule.ruleId}, version: ${this.props.rule.ruleVersion}`}>
        <RuleInput value={JSON.stringify(this.props.rule.content, null, 2)} onChange={this.handleChange} />
        <Button onClick={() => this.props.onUpdateClick(this.props.rule)}>Update</Button>
        <Button onClick={() => this.props.onRemoveClick(this.props.rule)}>Remove</Button>
      </Collapsible>
    );
  }
}

export default RuleView