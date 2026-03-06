import { useState, useRef, useEffect } from 'react'

function StatusBadge({ status }) {
  const colors = {
    Approved: 'bg-emerald-900/50 text-emerald-400',
    Denied: 'bg-red-900/50 text-red-400',
    Pending: 'bg-amber-900/50 text-amber-400',
    'Under Review': 'bg-blue-900/50 text-blue-400',
    Withdrawn: 'bg-slate-700 text-slate-400',
    Paid: 'bg-emerald-900/50 text-emerald-400',
  }
  return (
    <span className={`text-xs font-medium px-2 py-0.5 rounded-full ${colors[status] || 'bg-slate-700 text-slate-400'}`}>
      {status}
    </span>
  )
}

function MetricCard({ label, value, sub, color = 'teal' }) {
  const ring = { teal: 'ring-teal-800', amber: 'ring-amber-800', red: 'ring-red-800', blue: 'ring-blue-800' }
  const text = { teal: 'text-teal-400', amber: 'text-amber-400', red: 'text-red-400', blue: 'text-blue-400' }
  return (
    <div className={`bg-slate-800 rounded-xl p-4 shadow-sm ring-1 ${ring[color]}`}>
      <p className="text-xs text-slate-400 font-medium uppercase tracking-wide">{label}</p>
      <p className={`text-2xl font-bold mt-1 ${text[color]}`}>{value}</p>
      {sub && <p className="text-xs text-slate-500 mt-0.5">{sub}</p>}
    </div>
  )
}

function ToolSteps({ steps }) {
  const [expanded, setExpanded] = useState(false)
  if (!steps || steps.length === 0) return null

  const toolCalls = steps.filter(s => s.type === 'tool_call')
  const summary = toolCalls.length > 0
    ? `Used ${toolCalls.length} tool${toolCalls.length > 1 ? 's' : ''}: ${toolCalls.map(t => t.name).join(', ')}`
    : `${steps.length} intermediate step${steps.length > 1 ? 's' : ''}`

  return (
    <div className="mb-2 text-xs">
      <button
        onClick={() => setExpanded(!expanded)}
        className="flex items-center gap-1.5 text-slate-400 hover:text-teal-400 transition-colors"
      >
        <svg className={`w-3.5 h-3.5 transition-transform ${expanded ? 'rotate-90' : ''}`} fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
        </svg>
        <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.066 2.573c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.573 1.066c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.066-2.573c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z" />
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
        </svg>
        <span>{summary}</span>
      </button>
      {expanded && (
        <div className="mt-2 space-y-2 pl-5 border-l border-slate-700">
          {steps.map((step, i) => (
            <div key={i} className="bg-slate-900 rounded-lg p-2.5 ring-1 ring-slate-700">
              {step.type === 'tool_call' && (
                <>
                  <div className="flex items-center gap-1.5 text-teal-400 font-medium mb-1">
                    <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.066 2.573c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.573 1.066c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.066-2.573c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z" />
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
                    </svg>
                    {step.name}
                  </div>
                  {step.arguments && (
                    <pre className="text-slate-400 font-mono text-[11px] whitespace-pre-wrap break-all overflow-x-auto max-h-32 overflow-y-auto">{typeof step.arguments === 'string' ? step.arguments : JSON.stringify(step.arguments, null, 2)}</pre>
                  )}
                </>
              )}
              {step.type === 'tool_result' && (
                <>
                  <div className="flex items-center gap-1.5 text-slate-400 font-medium mb-1">
                    <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                    </svg>
                    Result{step.name ? `: ${step.name}` : ''}
                  </div>
                  <pre className="text-slate-500 font-mono text-[11px] whitespace-pre-wrap break-all overflow-x-auto max-h-32 overflow-y-auto">{typeof step.output === 'string' ? step.output : JSON.stringify(step.output, null, 2)}</pre>
                </>
              )}
              {step.type === 'thinking' && (
                <p className="text-slate-400 italic">{step.content}</p>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

function App() {
  const [messages, setMessages] = useState([])
  const [input, setInput] = useState('')
  const [loading, setLoading] = useState(false)
  const [conversationId, setConversationId] = useState(null)
  const [summary, setSummary] = useState(null)
  const [summaryLoading, setSummaryLoading] = useState(true)
  const [tab, setTab] = useState('dashboard')
  const messagesEndRef = useRef(null)

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages])

  useEffect(() => {
    fetch('/api/summary')
      .then(r => r.json())
      .then(setSummary)
      .catch(() => setSummary(null))
      .finally(() => setSummaryLoading(false))
  }, [])

  const sendMessage = async (e) => {
    e.preventDefault()
    const text = input.trim()
    if (!text || loading) return
    setInput('')
    setMessages((prev) => [...prev, { role: 'user', content: text }])
    setLoading(true)
    try {
      const res = await fetch('/api/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: text, conversation_id: conversationId }),
      })
      if (!res.ok) {
        const err = await res.json().catch(() => ({}))
        throw new Error(err.detail || `Server error: ${res.status}`)
      }
      const data = await res.json()
      setConversationId(data.conversation_id)
      setMessages((prev) => [...prev, { role: 'assistant', content: data.response, steps: data.steps || [] }])
    } catch (err) {
      setMessages((prev) => [
        ...prev,
        { role: 'assistant', content: `Error: ${err.message}` },
      ])
    } finally {
      setLoading(false)
    }
  }

  const totalAppeals = summary?.appeals_by_status?.reduce((s, r) => s + Number(r.count), 0) || 0
  const pendingAppeals = summary?.appeals_by_status?.find(r => r.appeal_status === 'Pending')?.count || 0
  const deniedAppeals = summary?.appeals_by_status?.find(r => r.appeal_status === 'Denied')?.count || 0
  const totalClaims = summary?.claims_by_status?.reduce((s, r) => s + Number(r.count), 0) || 0
  const memberInfo = summary?.member_count?.[0] || {}
  const overturnRate = summary?.appeals_by_status
    ? Math.round(summary.appeals_by_status.reduce((s, r) => s + Number(r.overturned || 0), 0) / totalAppeals * 100)
    : 0

  return (
    <div className="flex flex-col h-screen bg-slate-950">
      {/* Header */}
      <header className="bg-slate-900 border-b border-slate-800 text-white px-6 py-3 shadow-md flex items-center justify-between">
        <div>
          <h1 className="text-lg font-semibold tracking-tight">GLP-1 Claims & Appeals Assistant</h1>
          <p className="text-teal-400 text-xs">Utilization & Adjudication Analytics — Powered by Databricks</p>
        </div>
        <nav className="flex gap-1 bg-slate-800 rounded-lg p-0.5">
          {['dashboard', 'chat'].map(t => (
            <button
              key={t}
              onClick={() => setTab(t)}
              className={`px-3 py-1 rounded-md text-xs font-medium capitalize transition-colors ${
                tab === t ? 'bg-teal-600 text-white shadow-sm' : 'text-slate-400 hover:text-white'
              }`}
            >
              {t}
            </button>
          ))}
        </nav>
      </header>

      {/* Dashboard Tab */}
      {tab === 'dashboard' && (
        <main className="flex-1 overflow-y-auto p-6 max-w-6xl mx-auto w-full">
          {summaryLoading ? (
            <div className="flex items-center justify-center h-64 text-slate-500">Loading dashboard...</div>
          ) : !summary ? (
            <div className="flex items-center justify-center h-64 text-slate-500">Unable to load summary data</div>
          ) : (
            <>
              {/* KPI Cards */}
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
                <MetricCard label="Total Appeals" value={totalAppeals.toLocaleString()} sub={`${pendingAppeals} pending`} color="teal" />
                <MetricCard label="Total Claims" value={totalClaims.toLocaleString()} color="blue" />
                <MetricCard label="Overturn Rate" value={`${overturnRate}%`} sub="of decided appeals" color="amber" />
                <MetricCard label="Members" value={Number(memberInfo.members || 0).toLocaleString()} sub={`${memberInfo.medicare || 0} Medicare`} color="teal" />
              </div>

              <div className="grid md:grid-cols-2 gap-6 mb-6">
                {/* Appeals by Status */}
                <div className="bg-slate-800 rounded-xl shadow-sm ring-1 ring-slate-700 p-5">
                  <h2 className="text-sm font-semibold text-slate-200 mb-3">Appeals by Status</h2>
                  <div className="space-y-2">
                    {summary.appeals_by_status?.map(row => {
                      const pct = Math.round(Number(row.count) / totalAppeals * 100)
                      const barColor = {
                        Approved: 'bg-emerald-500', Denied: 'bg-red-500',
                        Pending: 'bg-amber-500', 'Under Review': 'bg-blue-500', Withdrawn: 'bg-slate-500'
                      }[row.appeal_status] || 'bg-slate-500'
                      return (
                        <div key={row.appeal_status}>
                          <div className="flex justify-between text-xs mb-0.5">
                            <span className="text-slate-300">{row.appeal_status}</span>
                            <span className="text-slate-500">{row.count} ({pct}%)</span>
                          </div>
                          <div className="h-2 bg-slate-700 rounded-full overflow-hidden">
                            <div className={`h-full rounded-full ${barColor}`} style={{ width: `${pct}%` }} />
                          </div>
                        </div>
                      )
                    })}
                  </div>
                </div>

                {/* Top Denial Reasons */}
                <div className="bg-slate-800 rounded-xl shadow-sm ring-1 ring-slate-700 p-5">
                  <h2 className="text-sm font-semibold text-slate-200 mb-3">Top Denial Reasons</h2>
                  <div className="space-y-2">
                    {summary.top_denial_reasons?.map((row, i) => (
                      <div key={i} className="flex items-center justify-between py-1.5 border-b border-slate-700 last:border-0">
                        <span className="text-xs text-slate-300 truncate mr-2">{row.reason}</span>
                        <span className="text-xs font-mono text-slate-400 shrink-0">{row.count}</span>
                      </div>
                    ))}
                  </div>
                </div>
              </div>

              {/* Claims Summary */}
              <div className="bg-slate-800 rounded-xl shadow-sm ring-1 ring-slate-700 p-5 mb-6">
                <h2 className="text-sm font-semibold text-slate-200 mb-3">Claims Summary</h2>
                <div className="overflow-x-auto">
                  <table className="w-full text-xs">
                    <thead>
                      <tr className="text-left text-slate-400 border-b border-slate-700">
                        <th className="pb-2 font-medium">Status</th>
                        <th className="pb-2 font-medium text-right">Count</th>
                        <th className="pb-2 font-medium text-right">Total Billed</th>
                        <th className="pb-2 font-medium text-right">Total Paid</th>
                      </tr>
                    </thead>
                    <tbody>
                      {summary.claims_by_status?.map(row => (
                        <tr key={row.status} className="border-b border-slate-700/50">
                          <td className="py-2"><StatusBadge status={row.status} /></td>
                          <td className="py-2 text-right text-slate-300">{Number(row.count).toLocaleString()}</td>
                          <td className="py-2 text-right text-slate-300">${Number(row.total_billed || 0).toLocaleString()}</td>
                          <td className="py-2 text-right text-slate-300">${Number(row.total_paid || 0).toLocaleString()}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>

              {/* Recent Appeals */}
              <div className="bg-slate-800 rounded-xl shadow-sm ring-1 ring-slate-700 p-5">
                <h2 className="text-sm font-semibold text-slate-200 mb-3">Recent Appeals</h2>
                <div className="overflow-x-auto">
                  <table className="w-full text-xs">
                    <thead>
                      <tr className="text-left text-slate-400 border-b border-slate-700">
                        <th className="pb-2 font-medium">Appeal ID</th>
                        <th className="pb-2 font-medium">Member</th>
                        <th className="pb-2 font-medium">Type</th>
                        <th className="pb-2 font-medium">Status</th>
                        <th className="pb-2 font-medium">Denial Reason</th>
                        <th className="pb-2 font-medium">Date</th>
                      </tr>
                    </thead>
                    <tbody>
                      {summary.recent_appeals?.map(row => (
                        <tr key={row.appeal_id} className="border-b border-slate-700/50 hover:bg-slate-700/30">
                          <td className="py-2 font-mono text-teal-400">{row.appeal_id}</td>
                          <td className="py-2 text-slate-300">{row.member_id}</td>
                          <td className="py-2 text-slate-300">{row.appeal_type}</td>
                          <td className="py-2"><StatusBadge status={row.appeal_status} /></td>
                          <td className="py-2 text-slate-400 truncate max-w-[200px]">{row.original_denial_reason}</td>
                          <td className="py-2 text-slate-500">{row.appeal_date}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>

              {/* CTA to chat */}
              <div className="mt-6 text-center">
                <button
                  onClick={() => setTab('chat')}
                  className="inline-flex items-center gap-2 bg-teal-600 text-white px-5 py-2.5 rounded-xl text-sm font-medium hover:bg-teal-500 transition-colors shadow-sm"
                >
                  Ask the GLP-1 Assistant
                  <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 12h.01M12 12h.01M16 12h.01M21 12c0 4.418-4.03 8-9 8a9.863 9.863 0 01-4.255-.949L3 20l1.395-3.72C3.512 15.042 3 13.574 3 12c0-4.418 4.03-8 9-8s9 3.582 9 8z" />
                  </svg>
                </button>
              </div>
            </>
          )}
        </main>
      )}

      {/* Chat Tab */}
      {tab === 'chat' && (
        <>
          <main className="flex-1 overflow-y-auto px-4 py-6 max-w-3xl w-full mx-auto">
            {messages.length === 0 && (
              <div className="text-center text-slate-500 mt-16">
                <div className="w-12 h-12 bg-teal-900/50 rounded-full flex items-center justify-center mx-auto mb-4">
                  <svg className="w-6 h-6 text-teal-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 12h.01M12 12h.01M16 12h.01M21 12c0 4.418-4.03 8-9 8a9.863 9.863 0 01-4.255-.949L3 20l1.395-3.72C3.512 15.042 3 13.574 3 12c0-4.418 4.03-8 9-8s9 3.582 9 8z" />
                  </svg>
                </div>
                <p className="text-base font-medium text-slate-300 mb-1">How can I help?</p>
                <p className="text-sm mb-6">Ask about GLP-1 claims, appeals, or utilization patterns.</p>
                <div className="flex flex-wrap justify-center gap-2">
                  {[
                    'What are the GLP-1 medications covered under Part D?',
                    'Show appeals related to Ozempic or Wegovy denials',
                    'What are common denial reasons for GLP-1 prescriptions?',
                    'How do I appeal a prior authorization denial for semaglutide?',
                  ].map((q) => (
                    <button
                      key={q}
                      onClick={() => setInput(q)}
                      className="text-xs bg-slate-800 border border-slate-700 rounded-full px-3.5 py-1.5 text-slate-400 hover:border-teal-500 hover:text-teal-400 transition-colors"
                    >
                      {q}
                    </button>
                  ))}
                </div>
              </div>
            )}

            {messages.map((msg, i) => (
              <div key={i} className={`flex mb-4 ${msg.role === 'user' ? 'justify-end' : 'justify-start'}`}>
                {msg.role === 'assistant' && (
                  <div className="w-7 h-7 bg-teal-900/50 rounded-full flex items-center justify-center mr-2 mt-1 shrink-0">
                    <svg className="w-3.5 h-3.5 text-teal-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9.75 17L9 20l-1 1h8l-1-1-.75-3M3 13h18M5 17h14a2 2 0 002-2V5a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z" />
                    </svg>
                  </div>
                )}
                <div
                  className={`max-w-[75%] rounded-2xl px-4 py-3 text-sm leading-relaxed whitespace-pre-wrap ${
                    msg.role === 'user'
                      ? 'bg-teal-600 text-white rounded-br-sm'
                      : 'bg-slate-800 text-slate-200 border border-slate-700 rounded-bl-sm'
                  }`}
                >
                  {msg.role === 'assistant' && <ToolSteps steps={msg.steps} />}
                  {msg.content}
                </div>
              </div>
            ))}

            {loading && (
              <div className="flex justify-start mb-4">
                <div className="w-7 h-7 bg-teal-900/50 rounded-full flex items-center justify-center mr-2 mt-1 shrink-0">
                  <svg className="w-3.5 h-3.5 text-teal-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9.75 17L9 20l-1 1h8l-1-1-.75-3M3 13h18M5 17h14a2 2 0 002-2V5a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z" />
                  </svg>
                </div>
                <div className="bg-slate-800 border border-slate-700 rounded-2xl rounded-bl-sm px-4 py-3">
                  <div className="flex gap-1.5">
                    <span className="w-2 h-2 bg-teal-400 rounded-full animate-bounce" style={{ animationDelay: '0ms' }} />
                    <span className="w-2 h-2 bg-teal-400 rounded-full animate-bounce" style={{ animationDelay: '150ms' }} />
                    <span className="w-2 h-2 bg-teal-400 rounded-full animate-bounce" style={{ animationDelay: '300ms' }} />
                  </div>
                </div>
              </div>
            )}
            <div ref={messagesEndRef} />
          </main>

          <footer className="border-t border-slate-800 bg-slate-900 px-4 py-3">
            <form onSubmit={sendMessage} className="max-w-3xl mx-auto flex gap-3">
              <input
                type="text"
                value={input}
                onChange={(e) => setInput(e.target.value)}
                placeholder="Ask about claims, appeals, or member info..."
                className="flex-1 border border-slate-700 bg-slate-800 text-slate-200 rounded-xl px-4 py-2.5 text-sm focus:outline-none focus:ring-2 focus:ring-teal-500 focus:border-transparent placeholder-slate-500"
                disabled={loading}
              />
              <button
                type="submit"
                disabled={loading || !input.trim()}
                className="bg-teal-600 text-white px-5 py-2.5 rounded-xl text-sm font-medium hover:bg-teal-500 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
              >
                Send
              </button>
            </form>
          </footer>
        </>
      )}
    </div>
  )
}

export default App
