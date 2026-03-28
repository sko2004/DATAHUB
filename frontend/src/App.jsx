import React, { useState, useEffect } from 'react';
import { 
  LayoutDashboard, Database, FolderGit2, Bot, Shield,  
  UploadCloud, Search, LogOut, ChevronRight, Activity, 
  FileJson, FileSpreadsheet, Loader2, Link, Server, CheckCircle2,
  XCircle, Send
} from 'lucide-react';

const API = 'http://localhost:8000';

function App() {
  const [token, setToken] = useState(localStorage.getItem('dh_token'));
  const [user, setUser] = useState(() => {
    const saved = localStorage.getItem('dh_user');
    return saved ? JSON.parse(saved) : null;
  });
  
  const [currentSection, setCurrentSection] = useState('overview');
  const [apiStatus, setApiStatus] = useState('checking');

  useEffect(() => {
    checkApi();
    const intv = setInterval(checkApi, 10000);
    return () => clearInterval(intv);
  }, []);

  const checkApi = async () => {
    try {
      const res = await fetch(`${API}/health`);
      if (res.ok) setApiStatus('online');
      else setApiStatus('offline');
    } catch { setApiStatus('offline'); }
  };

  const handleAuth = (newToken, newUser) => {
    setToken(newToken);
    setUser(newUser);
    localStorage.setItem('dh_token', newToken);
    localStorage.setItem('dh_user', JSON.stringify(newUser));
    setCurrentSection('overview');
  };

  const logout = () => {
    setToken(null);
    setUser(null);
    localStorage.removeItem('dh_token');
    localStorage.removeItem('dh_user');
  };

  if (!token) return <AuthPage onAuth={handleAuth} apiStatus={apiStatus} />;

  return (
    <div className="app-container">
      {/* Sidebar */}
      <aside className="sidebar">
        <div className="sidebar-header">
          <div className="logo-icon"><Database size={18} color="white" /></div>
          <h2>DataHub</h2>
        </div>
        
        <nav className="sidebar-nav">
          <NavItem icon={<LayoutDashboard size={18} />} label="Overview" active={currentSection === 'overview'} onClick={() => setCurrentSection('overview')} />
          {user?.role !== 'viewer' && (
            <NavItem icon={<UploadCloud size={18} />} label="Commit Data" active={currentSection === 'commit'} onClick={() => setCurrentSection('commit')} />
          )}
          <NavItem icon={<Search size={18} />} label="Explore Metadata" active={currentSection === 'explore'} onClick={() => setCurrentSection('explore')} />
          <NavItem icon={<FolderGit2 size={18} />} label="Projects & Commits" active={currentSection === 'projects'} onClick={() => setCurrentSection('projects')} />
          <NavItem icon={<Bot size={18} />} label="AI Data Agent" active={currentSection === 'ai'} onClick={() => setCurrentSection('ai')} />
          {user?.role === 'admin' && (
            <NavItem icon={<Shield size={18} />} label="DBA Console" active={currentSection === 'dba'} onClick={() => setCurrentSection('dba')} />
          )}
        </nav>

        <div style={{ marginTop: 'auto', borderTop: '1px solid var(--border-light)', paddingTop: '1.5rem' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '12px', marginBottom: '16px' }}>
            <div style={{ width: 36, height: 36, borderRadius: 18, background: 'rgba(99,102,241,0.2)', color: 'var(--accent-primary)', display: 'flex', alignItems: 'center', justifyContent: 'center', fontWeight: 'bold' }}>
              {user.username[0].toUpperCase()}
            </div>
            <div>
              <div style={{ fontWeight: 600, fontSize: '0.9rem' }}>{user.username}</div>
              <div style={{ fontSize: '0.8rem', color: 'var(--text-secondary)', textTransform: 'uppercase', letterSpacing: '0.05em' }}>{user.role}</div>
            </div>
          </div>
          <button onClick={logout} className="nav-item" style={{ color: 'var(--danger)' }}>
            <LogOut size={18} /> Sign Out
          </button>
        </div>
      </aside>

      {/* Main Content */}
      <main className="main-content">
        <header className="top-header">
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', color: 'var(--text-secondary)' }}>
            <Database size={16} /> <ChevronRight size={14} /> 
            <span style={{ color: 'white', fontWeight: 500, textTransform: 'capitalize' }}>
              {currentSection.replace('-', ' ')}
            </span>
          </div>
          <div className="status-indicator">
            <div className={`status-dot ${apiStatus !== 'online' ? 'offline' : ''}`} />
            {apiStatus === 'online' ? 'API Connected' : 'API Offline'}
          </div>
        </header>
        
        <div className="page-container">
          {currentSection === 'overview' && <OverviewTab token={token} />}
          {currentSection === 'commit' && <CommitTab token={token} onComplete={() => setCurrentSection('explore')} />}
          {currentSection === 'explore' && <ExploreTab token={token} />}
          {currentSection === 'projects' && <ProjectsTab token={token} />}
          {currentSection === 'ai' && <AiChatTab token={token} />}
        </div>
      </main>
    </div>
  );
}

function NavItem({ icon, label, active, onClick }) {
  return (
    <button className={`nav-item ${active ? 'active' : ''}`} onClick={onClick}>
      {icon} {label}
    </button>
  );
}

// -------------------------------------------------------------
// SECTIONS
// -------------------------------------------------------------
function OverviewTab({ token }) {
  const [stats, setStats] = useState(null);

  useEffect(() => {
    fetch(`${API}/metadata/stats/summary`, { headers: { Authorization: `Bearer ${token}` } })
      .then(r => r.json()).then(setStats).catch(console.error);
  }, [token]);

  if (!stats) return <p style={{ color: 'var(--text-muted)' }}>Loading network statistics...</p>;

  return (
    <div>
      <h1>Network Overview</h1>
      <p style={{ marginBottom: '2rem' }}>Real-time statistics of your decentralized data lake.</p>
      
      <div className="stats-grid">
        <div className="card stat-card">
          <span className="badge badge-purple" style={{ alignSelf: 'flex-start' }}>Projects</span>
          <div className="stat-value">{stats.total_projects}</div>
          <p>Active Repositories</p>
        </div>
        <div className="card stat-card">
          <span className="badge badge-blue" style={{ alignSelf: 'flex-start' }}>Commits</span>
          <div className="stat-value">{stats.total_commits}</div>
          <p>Data Version Points</p>
        </div>
        <div className="card stat-card">
          <span className="badge badge-green" style={{ alignSelf: 'flex-start' }}>Storage</span>
          <div className="stat-value">{(stats.total_storage_bytes / 1024).toFixed(1)}</div>
          <p>KB Deduplicated Data</p>
        </div>
        <div className="card stat-card">
          <span style={{ alignSelf: 'flex-start' }} className="badge" style={{ background: 'rgba(255,255,255,0.1)' }}>Scale</span>
          <div className="stat-value">{stats.total_rows_indexed.toLocaleString()}</div>
          <p>Data Rows Indexed</p>
        </div>
      </div>
    </div>
  );
}

function CommitTab({ token, onComplete }) {
  const [file, setFile] = useState(null);
  const [project, setProject] = useState('');
  const [msg, setMsg] = useState('');
  const [loading, setLoading] = useState(false);

  const handleDrop = (e) => {
    e.preventDefault();
    if (e.dataTransfer.files[0]) setFile(e.dataTransfer.files[0]);
  };

  const handleUpload = async (e) => {
    e.preventDefault();
    if (!file || !project || !msg) return alert('Please fill required fields.');
    setLoading(true);
    const fd = new FormData();
    fd.append('file', file); fd.append('project_name', project); fd.append('message', msg);
    
    try {
      const res = await fetch(`${API}/metadata/upload-and-commit`, {
        method: 'POST', headers: { Authorization: `Bearer ${token}` }, body: fd
      });
      if (!res.ok) throw new Error(await res.text());
      onComplete();
    } catch (err) { alert('Upload failed: ' + err.message); }
    finally { setLoading(false); }
  };

  return (
    <div style={{ maxWidth: 800 }}>
      <h1>Commit Data</h1>
      <p style={{ marginBottom: '2rem' }}>Upload CSV, JSON, or Parquet. Metadata is automatically extracted.</p>

      <form onSubmit={handleUpload} className="card">
        <div 
          className="dropzone" 
          onDragOver={e => e.preventDefault()} 
          onDrop={handleDrop}
          onClick={() => document.getElementById('fu').click()}
        >
          <UploadCloud className="dropzone-icon" size={48} />
          {file ? <h3 style={{ color: 'white' }}>{file.name}</h3> : <h3>Click or drag file here</h3>}
          <p>{file ? `${(file.size/1024).toFixed(1)} KB` : 'Supports .csv, .json, .parquet'}</p>
          <input id="fu" type="file" style={{ display: 'none' }} onChange={e => setFile(e.target.files[0])} accept=".csv,.json,.parquet" />
        </div>

        <div style={{ display: 'flex', gap: '1rem', marginTop: '1.5rem' }}>
          <div className="form-group" style={{ flex: 1 }}>
            <label className="form-label">Project Repository Name</label>
            <input required className="form-input" value={project} onChange={e=>setProject(e.target.value)} placeholder="e.g., ai-training-data" />
          </div>
          <div className="form-group" style={{ flex: 1 }}>
            <label className="form-label">Commit Message</label>
            <input required className="form-input" value={msg} onChange={e=>setMsg(e.target.value)} placeholder="e.g., Add initial dataset" />
          </div>
        </div>

        <button disabled={loading} type="submit" className="btn-primary" style={{ width: '100%', padding: '16px' }}>
          {loading ? <Loader2 className="spinner" /> : <UploadCloud />} 
          {loading ? 'Processing & Indexing...' : 'Commit File'}
        </button>
      </form>
    </div>
  );
}

function ExploreTab({ token }) {
  const [items, setItems] = useState([]);
  
  useEffect(() => {
    fetch(`${API}/metadata/`, { headers: { Authorization: `Bearer ${token}` } })
      .then(r => r.json()).then(setItems).catch(console.error);
  }, [token]);

  return (
    <div>
      <h1>Metadata Explorer</h1>
      <p style={{ marginBottom: '2rem' }}>Browse automatically extracted schemas and metrics.</p>
      
      <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
        {items.length === 0 && <p className="muted">No datasets found.</p>}
        {items.map(m => (
          <div key={m.id} className="card" style={{ display: 'flex', gap: '1rem', alignItems: 'flex-start' }}>
            <div style={{ padding: '12px', background: 'rgba(255,255,255,0.05)', borderRadius: '12px' }}>
              {m.file_type === 'csv' ? <FileSpreadsheet color="var(--accent-primary)" size={24}/> : <FileJson color="var(--success)" size={24}/>}
            </div>
            <div style={{ flex: 1 }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <h3>{m.file_name}</h3>
                <span className="badge badge-blue">{m.file_type.toUpperCase()}</span>
              </div>
              <p style={{ fontSize: '0.85rem', marginTop: '4px' }}>Commit: {m.commit_hash.slice(0,8)} · Indexed on {new Date(m.indexed_at).toLocaleDateString()}</p>
              
              <div style={{ display: 'flex', gap: '2rem', marginTop: '1rem', padding: '1rem', background: 'rgba(0,0,0,0.2)', borderRadius: '8px' }}>
                <div><div style={{ color: 'white', fontWeight: 600 }}>{m.row_count.toLocaleString()}</div><div style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>ROWS</div></div>
                <div><div style={{ color: 'white', fontWeight: 600 }}>{m.column_count}</div><div style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>COLUMNS</div></div>
                {m.ai_summary && (
                  <div style={{ flex: 1, borderLeft: '1px solid var(--border-light)', paddingLeft: '2rem' }}>
                    <div style={{ fontSize: '0.75rem', color: 'var(--accent-primary)', fontWeight: 600, marginBottom: '4px' }}>✦ AI SUMMARY</div>
                    <div style={{ fontSize: '0.85rem', color: 'var(--text-secondary)' }}>{m.ai_summary.substring(0, 150)}...</div>
                  </div>
                )}
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

function ProjectsTab({ token }) {
  const [projects, setProjects] = useState([]);
  
  useEffect(() => {
    fetch(`${API}/projects/`, { headers: { Authorization: `Bearer ${token}` } })
      .then(r => r.json()).then(setProjects).catch(console.error);
  }, [token]);

  return (
    <div>
      <h1>Projects & Commits</h1>
      <p style={{ marginBottom: '2rem' }}>Immutable history of your repositories.</p>
      <div className="stats-grid">
        {projects.length === 0 && <p className="muted">No projects found.</p>}
        {projects.map(p => (
          <div key={p.id} className="card">
            <h3 style={{ color: 'white' }}>{p.name}</h3>
            <p style={{ fontSize: '0.85rem', marginTop: '8px' }}>{p.description || 'No description available.'}</p>
            <div style={{ marginTop: '1rem', fontSize: '0.8rem', color: 'var(--accent-primary)' }}>Owner: {p.owner}</div>
          </div>
        ))}
      </div>
    </div>
  );
}

function AiChatTab({ token }) {
  const [msg, setMsg] = useState('');
  const [chat, setChat] = useState([]);
  const [loading, setLoading] = useState(false);

  const send = async (e) => {
    e.preventDefault();
    if (!msg.trim()) return;
    const q = msg;
    setMsg('');
    setChat(c => [...c, { role: 'user', text: q }]);
    setLoading(true);
    
    try {
      const res = await fetch(`${API}/ai/chat`, {
        method: 'POST', headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ question: q })
      });
      const data = await res.json();
      setChat(c => [...c, { role: 'bot', text: data.answer }]);
    } catch {
      setChat(c => [...c, { role: 'bot', text: 'Failed to connect to AI.' }]);
    } finally { setLoading(false); }
  };

  return (
    <div style={{ height: 'calc(100vh - 160px)', display: 'flex', flexDirection: 'column' }}>
      <h1>AI Data Agent</h1>
      <p style={{ marginBottom: '1.5rem' }}>Ask questions about any of your committed datasets.</p>
      
      <div className="card" style={{ flex: 1, display: 'flex', flexDirection: 'column', padding: '1rem', overflow: 'hidden' }}>
        <div style={{ flex: 1, overflowY: 'auto', padding: '1rem', display: 'flex', flexDirection: 'column', gap: '1rem' }}>
          <div style={{ alignSelf: 'flex-start', background: 'rgba(99,102,241,0.1)', border: '1px solid rgba(99,102,241,0.2)', padding: '1rem', borderRadius: '12px', maxWidth: '80%' }}>
            Hi! I'm your DataHub AI Agent. Ask me anything about the datasets you've indexed!
          </div>
          {chat.map((m, i) => (
            <div key={i} style={{ 
              alignSelf: m.role === 'user' ? 'flex-end' : 'flex-start',
              background: m.role === 'user' ? 'var(--accent-primary)' : 'rgba(255,255,255,0.05)',
              color: 'white', padding: '1rem', borderRadius: '12px', maxWidth: '80%', wordBreak: 'break-word',
              border: m.role === 'user' ? 'none' : '1px solid var(--border-light)'
            }}>
              {m.text}
            </div>
          ))}
          {loading && <div style={{ alignSelf: 'flex-start', color: 'var(--text-muted)' }}>Agent is thinking...</div>}
        </div>
        
        <form onSubmit={send} style={{ display: 'flex', gap: '10px', marginTop: '1rem' }}>
          <input className="form-input" style={{ flex: 1 }} value={msg} onChange={e=>setMsg(e.target.value)} placeholder="e.g. Give me a summary of the ML project..." />
          <button type="submit" className="btn-primary" disabled={loading}><Send size={18} /></button>
        </form>
      </div>
    </div>
  );
}

// -------------------------------------------------------------
// AUTH PAGE
// -------------------------------------------------------------
function AuthPage({ onAuth, apiStatus }) {
  const [isLogin, setIsLogin] = useState(true);
  const [user, setUser] = useState('');
  const [pass, setPass] = useState('');
  const [email, setEmail] = useState('');
  const [err, setErr] = useState('');

  const submit = async (e) => {
    e.preventDefault();
    setErr('');
    try {
      if (isLogin) {
        const fd = new URLSearchParams(); fd.append('username', user); fd.append('password', pass);
        const res = await fetch(`${API}/auth/login`, { method: 'POST', body: fd, headers: { 'Content-Type': 'application/x-www-form-urlencoded' }});
        if (!res.ok) throw new Error('Invalid credentials');
        const data = await res.json();
        onAuth(data.access_token, data.user);
      } else {
        const res = await fetch(`${API}/auth/register`, { method: 'POST', headers: { 'Content-Type':'application/json' }, body: JSON.stringify({ username: user, password: pass, email, role: 'analyst' }) });
        if (!res.ok) throw new Error('Registration failed');
        setIsLogin(true);
        alert('Registered! Please sign in.');
      }
    } catch (e) { setErr(e.message); }
  };

  return (
    <div className="auth-container">
      <div className="auth-card">
        <div style={{ textAlign: 'center', marginBottom: '2rem' }}>
          <div style={{ display: 'inline-flex', alignItem: 'center', justifyContent: 'center', width: 48, height: 48, background: 'linear-gradient(135deg, var(--accent-primary), #a855f7)', borderRadius: '12px', marginBottom: '1rem', boxShadow: '0 0 20px rgba(99,102,241,0.4)' }}>
            <Database color="white" size={24} style={{ marginTop: 12 }}/>
          </div>
          <h1>DataHub</h1>
          <p>Decentralized Metadata Intelligence</p>
        </div>

        <div style={{ display: 'flex', gap: '8px', marginBottom: '2rem', background: 'rgba(0,0,0,0.2)', padding: '4px', borderRadius: '12px' }}>
          <button onClick={() => setIsLogin(true)} style={{ flex: 1, padding: '8px', background: isLogin ? 'var(--bg-card)' : 'transparent', border: 'none', color: isLogin ? 'white' : 'var(--text-muted)', borderRadius: '8px', cursor: 'pointer', fontWeight: 600 }}>Login</button>
          <button onClick={() => setIsLogin(false)} style={{ flex: 1, padding: '8px', background: !isLogin ? 'var(--bg-card)' : 'transparent', border: 'none', color: !isLogin ? 'white' : 'var(--text-muted)', borderRadius: '8px', cursor: 'pointer', fontWeight: 600 }}>Register</button>
        </div>

        {err && <div style={{ color: '#fca5a5', background: 'rgba(239, 68, 68, 0.1)', padding: '12px', borderRadius: '8px', marginBottom: '1rem', fontSize: '0.9rem', border: '1px solid rgba(239, 68, 68, 0.2)' }}>{err}</div>}

        <form onSubmit={submit}>
          <div className="form-group">
            <label className="form-label">Username</label>
            <input required className="form-input" value={user} onChange={e=>setUser(e.target.value)} />
          </div>
          {!isLogin && (
            <div className="form-group">
              <label className="form-label">Email</label>
              <input required type="email" className="form-input" value={email} onChange={e=>setEmail(e.target.value)} />
            </div>
          )}
          <div className="form-group">
            <label className="form-label">Password</label>
            <input required type="password" className="form-input" value={pass} onChange={e=>setPass(e.target.value)} />
          </div>
          
          <button type="submit" className="btn-primary" style={{ width: '100%', padding: '14px', marginTop: '1rem' }}>
            {isLogin ? 'Sign In Serverly' : 'Create Account'}
          </button>
        </form>

        {isLogin && (
          <div style={{ marginTop: '2rem', textAlign: 'center', fontSize: '0.85rem' }}>
            <p className="muted" style={{ marginBottom: '1rem' }}>Test Accounts</p>
            <div style={{ display: 'flex', gap: '8px', justifyContent: 'center' }}>
              <button className="badge badge-purple" style={{ cursor: 'pointer', border: 'none' }} onClick={() => {setUser('admin_user');setPass('Admin@123');}}>👑 Admin</button>
              <button className="badge badge-blue" style={{ cursor: 'pointer', border: 'none' }} onClick={() => {setUser('analyst_user');setPass('Analyst@123');}}>📊 Analyst</button>
              <button className="badge badge-green" style={{ cursor: 'pointer', border: 'none' }} onClick={() => {setUser('viewer_user');setPass('Viewer@123');}}>👁 Viewer</button>
            </div>
          </div>
        )}

        <div style={{ marginTop: '2.5rem', textAlign: 'center' }}>
          <div className="status-indicator" style={{ display: 'inline-flex', background: 'transparent', border: 'none' }}>
            <div className={`status-dot ${apiStatus !== 'online' ? 'offline' : ''}`} />
            {apiStatus === 'online' ? 'Backend Connected (Port 8000)' : 'Backend Offline'}
          </div>
        </div>
      </div>
    </div>
  );
}

export default App;
