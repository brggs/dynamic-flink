import React from 'react';
import styled from 'styled-components'

import './App.css';
import DynamicFlinkController from './DynamicFlinkController';

const Header = styled.header` 
  background-color: #26C7D5;
  height: 60px;
  padding: 20px;
  color: white;
`

function App() {
  return (
    <div className="App">
      <Header className="App-header">
        <h1 className="App-title">Dynamic Flink Demo</h1>
      </Header>
      <DynamicFlinkController />
    </div>
  );
}

export default App;
