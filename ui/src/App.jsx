import { useState } from 'react'
import Client from './components/Client'
import Dashboard from './components/Dashboard'

function App() {
  const [view, setView] = useState('dashboard')

  return (
    <div>
      {view === 'dashboard' ? (
        <Dashboard onOpenClient={() => setView('client')} />
      ) : (
        <Client onClose={() => setView('dashboard')} />
      )}
    </div>
  )
}

export default App
