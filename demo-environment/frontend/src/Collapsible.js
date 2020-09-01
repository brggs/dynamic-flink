import React, { Component } from 'react'
import styled from 'styled-components'

const Header = styled.div`
    cursor: pointer;
    border: solid 1px #f2f2f2;
    padding: 15px;
    background-color: #0089CC;
    color: #FFF;
    font-family: verdana;
`

const Content = styled.div`
    cursor: pointer;
    border-left: solid 1px #f2f2f2;
    border-right: solid 1px #f2f2f2;
    border-bottom: solid 1px #f2f2f2;
    border-radius: 0 0 5px 5px;
    padding: 15px;
    font-family: verdana;
    font-size: 14px;
`

class Collapsible extends Component {
    constructor(props) {
        super(props);
        this.state = {
            open: false
        }
        this.togglePanel = this.togglePanel.bind(this);
    }
    togglePanel(e) {
        this.setState({ open: !this.state.open })
    }
    render() {
        return (<div>
            <Header onClick={(e) => this.togglePanel(e)}>{this.props.title}</Header>
            {this.state.open ? (
                <Content>{this.props.children}</Content>
            ) : null}
        </div>);
    }
}

export default Collapsible