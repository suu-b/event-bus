import { useState, useEffect, useRef } from 'react'

function Client({ onClose }) {
  const [clientId] = useState(() => "CL-" + Math.floor(Math.random() * 10000))
  const [task, setTask] = useState('')
  const [logs, setLogs] = useState([])
  const eventSourceRef = useRef(null)

  const addLog = (msg) => {
    setLogs(prev => [...prev, { time: new Date().toLocaleTimeString(), msg }])
  }

  const send = async () => {
    try {
      const res = await fetch("http://localhost:8000/request", {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          id: "REQ-" + Math.random().toString(36).substring(2, 10),
          client_id: clientId,
          content: task
        })
      })

      const data = await res.json()
      const requestId = data.request_id

      addLog(`Request submitted: ${requestId}`)
      setTask('')

      startStream(requestId)

    } catch (err) {
      console.error(err)
      addLog("Failed to send request")
    }
  }

  const startStream = (requestId) => {
    if (eventSourceRef.current) {
      eventSourceRef.current.close()
    }

    const url = `http://localhost:8000/stream/${requestId}`
    const evtSource = new EventSource(url)
    eventSourceRef.current = evtSource

    evtSource.onmessage = (event) => {
      addLog(`Update: ${event.data}`)
    }

    evtSource.onerror = () => {
      addLog("Stream closed")
      evtSource.close()
    }
  }

  useEffect(() => {
    return () => {
      if (eventSourceRef.current) {
        eventSourceRef.current.close()
      }
    }
  }, [])

  return (
    <div className="container">
      <h2>Client View</h2>
      <p>Client ID: {clientId}</p>

      <input
        type="text"
        value={task}
        onChange={(e) => setTask(e.target.value)}
        placeholder="Enter task description"
      />
      <div className="actions">
        <button onClick={send}>Send</button>
        <button className="secondary" onClick={onClose}>Close Client</button>
      </div>

      <div className="logs">
        {logs.map((log, index) => (
          <div key={index}>{log.time} - {log.msg}</div>
        ))}
      </div>
    </div>
  )
}

export default Client
