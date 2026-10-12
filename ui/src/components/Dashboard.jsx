import { useState, useEffect } from 'react'

function Dashboard({ onOpenClient }) {
  const [history, setHistory] = useState([])

  const fetchHistory = async () => {
    try {
      const res = await fetch("http://localhost:8000/history")
      const data = await res.json()
      setHistory(data)
    } catch (err) {
      console.error("Error fetching history")
    }
  }

  useEffect(() => {
    fetchHistory()
    const interval = setInterval(fetchHistory, 2000)
    return () => clearInterval(interval)
  }, [])

  return (
    <div className="container">
      <div className="header">
        <h2>Request History</h2>
        <button onClick={onOpenClient}>New Client</button>
      </div>

      <table>
        <thead>
          <tr>
            <th>Request ID</th>
            <th>Client ID</th>
            <th>Server ID</th>
            <th>Duration</th>
            <th>Created At</th>
            <th>Completed At</th>
          </tr>
        </thead>
        <tbody>
          {history.map((item, index) => (
            <tr key={index}>
              <td>{item.request_id}</td>
              <td>{item.client_id}</td>
              <td>{item.server_id}</td>
              <td>{item.duration}</td>
              <td>{new Date(item.created_at * 1000).toLocaleString()}</td>
              <td>{new Date(item.completed_at * 1000).toLocaleString()}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

export default Dashboard
