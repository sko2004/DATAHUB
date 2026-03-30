import React, { useState, useEffect } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { 
  LayoutDashboard, Database, FolderGit2, Bot, Shield,  
  UploadCloud, Search, LogOut, ChevronRight, Activity, 
  FileJson, FileSpreadsheet, Loader2, Link, Server, CheckCircle2,
  XCircle, Send, ArrowLeft
} from 'lucide-react';

const API = 'http://localhost:8000';
const DEFAULT_STATS = {
  total_projects: 0,
  total_commits: 0,
  total_storage_bytes: 0,
  total_rows_indexed: 0,
};

function createAuthHeaders(token, extraHeaders = {}) {
  return token
    ? { ...extraHeaders, Authorization: `Bearer ${token}` }
    : extraHeaders;
}

async function parseJsonResponse(response) {
  const text = await response.text();
  let data = null;

  if (text) {
    try {
      data = JSON.parse(text);
    } catch {
      data = text;
    }
  }

  if (!response.ok) {
    const message =
      (data && typeof data === 'object' && (data.detail || data.message)) ||
      response.statusText ||
      'Request failed';
    const error = new Error(message);
    error.status = response.status;
    error.data = data;
    throw error;
  }

  return data;
}

async function fetchJson(url, options = {}) {
  const response = await fetch(url, options);
  return parseJsonResponse(response);
}

function handleUnauthorizedError(error, onUnauthorized) {
  if (error?.status === 401) {
    onUnauthorized?.();
    return true;
  }
  return false;
}

function formatNumber(value) {
  const num = Number(value ?? 0);
  return Number.isFinite(num) ? num.toLocaleString() : '0';
}

function formatFixed(value, digits = 1) {
  const num = Number(value ?? 0);
  return Number.isFinite(num) ? num.toFixed(digits) : (0).toFixed(digits);
}

function formatDate(value) {
  if (!value) return 'Unknown date';
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? 'Unknown date' : date.toLocaleDateString();
}

function formatDateTime(value) {
  if (!value) return 'Unknown date';
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? 'Unknown date' : date.toLocaleString();
}

function App() {
  const [token, setToken] = useState(localStorage.getItem('dh_token'));
  const [user, setUser] = useState(() => {
    const saved = localStorage.getItem('dh_user');
    return saved ? JSON.parse(saved) : null;
  });
  
  const [currentSection, setCurrentSection] = useState('overview');
  const [selectedMetadataId, setSelectedMetadataId] = useState(null);
  const [apiStatus, setApiStatus] = useState('checking');
  const [authNotice, setAuthNotice] = useState('');

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
    setAuthNotice('');
    localStorage.setItem('dh_token', newToken);
    localStorage.setItem('dh_user', JSON.stringify(newUser));
    setCurrentSection('overview');
    setSelectedMetadataId(null);
  };

  const logout = (notice) => {
    const noticeMsg = typeof notice === 'string' ? notice : '';
    setToken(null);
    setUser(null);
    setAuthNotice(noticeMsg);
    localStorage.removeItem('dh_token');
    localStorage.removeItem('dh_user');
    setCurrentSection('overview');
    setSelectedMetadataId(null);
  };

  const handleUnauthorized = () => logout('Your session expired. Please sign in again.');

  useEffect(() => {
    if (!token) return;

    let cancelled = false;

    async function validateSession() {
      try {
        const currentUser = await fetchJson(`${API}/auth/me`, {
          headers: createAuthHeaders(token),
        });
        if (cancelled) return;
        setUser(currentUser);
        localStorage.setItem('dh_user', JSON.stringify(currentUser));
      } catch (error) {
        console.error(error);
        if (cancelled) return;
        handleUnauthorizedError(error, handleUnauthorized);
      }
    }

    validateSession();

    return () => {
      cancelled = true;
    };
  }, [token]);

  if (!token) return <AuthPage onAuth={handleAuth} apiStatus={apiStatus} notice={authNotice} />;

  return (
    <div className="app-container">
      {/* Sidebar */}
      <aside className="sidebar">
        <div className="sidebar-header">
          <div className="logo-icon"><Database size={18} color="white" /></div>
          <h2>DataHub</h2>
        </div>
        
        <nav className="sidebar-nav">
          <NavItem icon={<LayoutDashboard size={18} />} label="Overview" active={currentSection === 'overview'} onClick={() => { setCurrentSection('overview'); setSelectedMetadataId(null); }} />
          {user?.role && user.role !== 'viewer' && (
            <NavItem icon={<UploadCloud size={18} />} label="Commit Data" active={currentSection === 'commit'} onClick={() => { setCurrentSection('commit'); setSelectedMetadataId(null); }} />
          )}
          <NavItem icon={<Search size={18} />} label="Explore Metadata" active={currentSection === 'explore'} onClick={() => { setCurrentSection('explore'); setSelectedMetadataId(null); }} />
          <NavItem icon={<FolderGit2 size={18} />} label="Projects & Commits" active={currentSection === 'projects'} onClick={() => { setCurrentSection('projects'); setSelectedMetadataId(null); }} />
          <NavItem icon={<Bot size={18} />} label="AI Data Agent" active={currentSection === 'ai'} onClick={() => { setCurrentSection('ai'); setSelectedMetadataId(null); }} />
          {user?.role === 'admin' && (
            <NavItem icon={<Shield size={18} />} label="DBA Console" active={currentSection === 'dba'} onClick={() => { setCurrentSection('dba'); setSelectedMetadataId(null); }} />
          )}
        </nav>

        <div style={{ marginTop: 'auto', borderTop: '1px solid var(--border-light)', paddingTop: '1.5rem' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '12px', marginBottom: '16px' }}>
            <div style={{ width: 36, height: 36, borderRadius: 18, background: 'rgba(99,102,241,0.2)', color: 'var(--accent-primary)', display: 'flex', alignItems: 'center', justifyContent: 'center', fontWeight: 'bold' }}>
              {user?.username?.[0]?.toUpperCase() || '?'}
            </div>
            <div>
              <div style={{ fontWeight: 600, fontSize: '0.9rem' }}>{user?.username || 'User'}</div>
              <div style={{ fontSize: '0.8rem', color: 'var(--text-secondary)', textTransform: 'uppercase', letterSpacing: '0.05em' }}>{user?.role || 'unknown'}</div>
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
          {currentSection === 'overview' && <OverviewTab token={token} onUnauthorized={handleUnauthorized} onNavigate={setCurrentSection} />}
          {currentSection === 'commit' && <CommitTab token={token} onUnauthorized={handleUnauthorized} onComplete={() => { setCurrentSection('explore'); setSelectedMetadataId(null); }} />}
          {currentSection === 'explore' && (
            selectedMetadataId ? 
              <DatasetDetail token={token} onUnauthorized={handleUnauthorized} metadataId={selectedMetadataId} onBack={() => setSelectedMetadataId(null)} /> :
              <ExploreTab token={token} onUnauthorized={handleUnauthorized} onSelect={setSelectedMetadataId} />
          )}
          {currentSection === 'projects' && <ProjectsTab token={token} onUnauthorized={handleUnauthorized} />}
          {currentSection === 'ai' && <AiChatTab token={token} onUnauthorized={handleUnauthorized} />}
          {currentSection === 'dba' && <DbaConsoleTab token={token} onUnauthorized={handleUnauthorized} />}
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
function OverviewTab({ token, onUnauthorized, onNavigate }) {
  const [stats, setStats] = useState(DEFAULT_STATS);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  useEffect(() => {
    let cancelled = false;

    async function loadStats() {
      setLoading(true);
      setError('');

      try {
        const data = await fetchJson(`${API}/metadata/stats/summary`, {
          headers: createAuthHeaders(token),
        });
        if (cancelled) return;
        setStats({ ...DEFAULT_STATS, ...(data || {}) });
      } catch (err) {
        console.error(err);
        if (cancelled) return;
        if (handleUnauthorizedError(err, onUnauthorized)) return;
        setStats(DEFAULT_STATS);
        setError(err.message || 'Unable to load network statistics.');
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    loadStats();

    return () => {
      cancelled = true;
    };
  }, [token, onUnauthorized]);

  if (loading) return <p style={{ color: 'var(--text-muted)' }}>Loading network statistics...</p>;

  return (
    <div>
      <h1>Network Overview</h1>
      <p style={{ marginBottom: '2rem' }}>Real-time statistics of your decentralized data lake.</p>
      {error && <p style={{ color: 'var(--danger)', marginBottom: '1.5rem' }}>{error}</p>}
      
      <div className="stats-grid">
        <div 
          className="card stat-card interactive" 
          onClick={() => onNavigate('projects')}
          style={{ cursor: 'pointer', transition: 'transform 0.2s' }}
        >
          <span className="badge badge-purple" style={{ alignSelf: 'flex-start' }}>Projects</span>
          <div className="stat-value">{formatNumber(stats.total_projects)}</div>
          <p>Active Repositories</p>
        </div>
        
        <div 
          className="card stat-card interactive" 
          onClick={() => onNavigate('projects')}
          style={{ cursor: 'pointer', transition: 'transform 0.2s' }}
        >
          <span className="badge badge-blue" style={{ alignSelf: 'flex-start' }}>Commits</span>
          <div className="stat-value">{formatNumber(stats.total_commits)}</div>
          <p>Data Version Points</p>
        </div>
        
        <div 
          className="card stat-card interactive" 
          onClick={() => onNavigate('explore')}
          style={{ cursor: 'pointer', transition: 'transform 0.2s' }}
        >
          <span className="badge badge-green" style={{ alignSelf: 'flex-start' }}>Storage</span>
          <div className="stat-value">{formatFixed((Number(stats.total_storage_bytes) || 0) / 1024, 1)}</div>
          <p>KB Deduplicated Data</p>
        </div>
        
        <div 
          className="card stat-card interactive" 
          onClick={() => onNavigate('explore')}
          style={{ cursor: 'pointer', transition: 'transform 0.2s' }}
        >
          <span style={{ alignSelf: 'flex-start', background: 'rgba(255,255,255,0.1)' }} className="badge">Scale</span>
          <div className="stat-value">{formatNumber(stats.total_rows_indexed)}</div>
          <p>Data Rows Indexed</p>
        </div>
      </div>
    </div>
  );
}

function CommitTab({ token, onComplete, onUnauthorized }) {
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
        method: 'POST', headers: createAuthHeaders(token), body: fd
      });
      if (res.status === 401) {
        onUnauthorized?.();
        return;
      }
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

function ExploreTab({ token, onSelect, onUnauthorized }) {
  const [items, setItems] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  
  useEffect(() => {
    let cancelled = false;

    async function loadItems() {
      setLoading(true);
      setError('');

      try {
        const data = await fetchJson(`${API}/metadata/`, {
          headers: createAuthHeaders(token),
        });
        if (cancelled) return;
        setItems(Array.isArray(data) ? data : []);
      } catch (err) {
        console.error(err);
        if (cancelled) return;
        if (handleUnauthorizedError(err, onUnauthorized)) return;
        setItems([]);
        setError(err.message || 'Unable to load datasets.');
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    loadItems();

    return () => {
      cancelled = true;
    };
  }, [token, onUnauthorized]);

  return (
    <div>
      <h1>Metadata Explorer</h1>
      <p style={{ marginBottom: '2rem' }}>Browse automatically extracted schemas and metrics.</p>
      
      <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
        {loading && <p className="muted">Loading datasets...</p>}
        {!loading && error && <p style={{ color: 'var(--danger)' }}>{error}</p>}
        {!loading && !error && items.length === 0 && <p className="muted">No datasets found.</p>}
        {items.map(m => (
          <div key={m.id} className="card dataset-card" onClick={() => onSelect(m.id)} style={{ display: 'flex', gap: '1rem', alignItems: 'flex-start', cursor: 'pointer' }}>
            <div style={{ padding: '12px', background: 'rgba(255,255,255,0.05)', borderRadius: '12px' }}>
              {m.file_type === 'csv' ? <FileSpreadsheet color="var(--accent-primary)" size={24}/> : <FileJson color="var(--success)" size={24}/>}
            </div>
            <div style={{ flex: 1 }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <h3 style={{ textTransform: 'capitalize' }}>{m.project_name || m.file_name}</h3>
                <span className="badge badge-blue">{(m.file_type || 'unknown').toUpperCase()}</span>
              </div>
              <p style={{ fontSize: '0.85rem', marginTop: '4px' }}>File: {m.file_name} · Commit: {(m.target_hash || '').slice(0,8)} · Indexed on {new Date(m.indexed_at).toLocaleDateString()}</p>
              
              <div style={{ display: 'flex', gap: '2rem', marginTop: '1rem', padding: '1rem', background: 'rgba(0,0,0,0.2)', borderRadius: '8px' }}>
                <div><div style={{ color: 'white', fontWeight: 600 }}>{formatNumber(m.row_count)}</div><div style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>ROWS</div></div>
                <div><div style={{ color: 'white', fontWeight: 600 }}>{formatNumber(m.column_count)}</div><div style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>COLUMNS</div></div>
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

function DatasetDetail({ token, metadataId, onBack, onUnauthorized }) {
  const [meta, setMeta] = useState(null);
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function load() {
      try {
        const [metaData, rowData] = await Promise.all([
          fetchJson(`${API}/metadata/${metadataId}`, { headers: createAuthHeaders(token) }),
          fetchJson(`${API}/metadata/${metadataId}/data`, { headers: createAuthHeaders(token) })
        ]);
        setMeta(metaData);
        setData(Array.isArray(rowData) ? rowData : []);
      } catch (e) {
        if (handleUnauthorizedError(e, onUnauthorized)) return;
        alert(e.message);
      } finally {
        setLoading(false);
      }
    }
    load();
  }, [metadataId, token, onUnauthorized]);

  if (loading) return <div style={{ display: 'flex', gap: '12px', color: 'var(--text-secondary)', padding: '2rem' }}><Loader2 className="spinner" /> Loading detailed records...</div>;
  if (!meta) return <div>Error loading dataset.</div>;

  return (
    <div className="dataset-detail">
      <button onClick={onBack} className="nav-item" style={{ marginBottom: '1.5rem', width: 'auto', display: 'flex', gap: '8px', paddingLeft: 0, background: 'transparent' }}>
        <ArrowLeft size={18} /> Back to Metadata Explorer
      </button>

      <header className="detail-header">
        <div>
          <h1 style={{ display: 'flex', alignItems: 'center', gap: '12px', textTransform: 'capitalize' }}>
            {meta.project_name || meta.file_name} <span className="badge badge-purple" style={{ fontSize: '0.9rem' }}>{String(meta.file_type || 'unknown').toUpperCase()}</span>
          </h1>
          <p style={{ marginTop: '0.5rem' }}>File: {meta.file_name} · Indexed: {new Date(meta.indexed_at).toLocaleString()} · Commit: {meta.target_hash}</p>
        </div>
      </header>

      <div className="stats-grid" style={{ marginBottom: '2.5rem' }}>
        <div className="card stat-card compact">
          <div className="stat-value">{formatNumber(meta.row_count)}</div>
          <p>Rows Indexed</p>
        </div>
        <div className="card stat-card compact">
          <div className="stat-value">{formatNumber(meta.column_count)}</div>
          <p>Total Columns</p>
        </div>
        <div className="card stat-card compact">
          <div className="stat-value">{(meta.blob_hash || "").slice(0, 10)}</div>
          <p>CAS Blob ID</p>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
          <button
            className="btn-primary"
            style={{ padding: '12px 20px', fontSize: '0.85rem' }}
            onClick={async () => {
              try {
                const res = await fetch(`${API}/metadata/${metadataId}/download`, {
                  headers: createAuthHeaders(token)
                });
                if (res.status === 401) {
                  onUnauthorized?.();
                  return;
                }
                if (!res.ok) throw new Error('Download failed');
                const blob = await res.blob();
                const url = URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = url;
                a.download = meta.file_name || `data_${metadataId}`;
                a.click();
                URL.revokeObjectURL(url);
              } catch (e) { alert(e.message); }
            }}
          >
            <UploadCloud size={16} /> Download File
          </button>
        </div>
      </div>

      <div className="card" style={{ marginBottom: '2.5rem', background: 'linear-gradient(135deg, rgba(99, 102, 241, 0.1), transparent)', borderColor: 'rgba(99, 102, 241, 0.2)' }}>
        <div style={{ fontSize: '0.7rem', color: 'var(--accent-primary)', fontWeight: 800, marginBottom: '0.75rem', letterSpacing: '0.1em' }}>✦ AI GENERATED REPORT</div>
        <p style={{ fontSize: '1.1rem', color: 'white', lineHeight: 1.6, fontWeight: 400 }}>{meta.ai_summary || 'No AI summary available for this dataset.'}</p>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 2fr', gap: '2rem', marginBottom: '2.5rem' }}>
        <div className="card">
          <h3 style={{ marginBottom: '1.5rem', display: 'flex', alignItems: 'center', gap: '8px', color: 'white' }}><Link size={18} /> Schema Definition</h3>
          <div style={{ maxHeight: '450px', overflowY: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse' }}>
              <thead>
                <tr>
                  <th style={{ textAlign: 'left', padding: '12px', borderBottom: '1px solid var(--border-light)', color: 'var(--text-secondary)' }}>Column Name</th>
                  <th style={{ textAlign: 'left', padding: '12px', borderBottom: '1px solid var(--border-light)', color: 'var(--text-secondary)' }}>Dtype</th>
                </tr>
              </thead>
              <tbody>
                {Object.entries(meta.columns_schema || {}).map(([col, type]) => (
                  <tr key={col}>
                    <td style={{ padding: '12px', borderBottom: '1px solid var(--border-light)', fontWeight: 500, fontSize: '0.9rem' }}>{col}</td>
                    <td style={{ padding: '12px', borderBottom: '1px solid var(--border-light)' }}><span className="badge badge-blue" style={{ fontSize: '0.7rem' }}>{type}</span></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        <div className="card">
          <h3 style={{ marginBottom: '1.5rem', display: 'flex', alignItems: 'center', gap: '8px', color: 'white' }}><Activity size={18} /> Deep Statistical Review</h3>
          <div style={{ maxHeight: '450px', overflowY: 'auto' }}>
            {Object.entries(meta.statistics || {}).map(([col, s]) => (
              <div key={col} style={{ marginBottom: '1rem', padding: '1.25rem', background: 'rgba(255,255,255,0.03)', borderRadius: '12px', border: '1px solid var(--border-light)' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '1rem' }}>
                  <span style={{ fontWeight: 600, color: 'white', fontSize: '1rem' }}>{col}</span>
                  <span className="badge badge-blue">{s.dtype}</span>
                </div>
                {s.mean !== undefined ? (
                  <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: '1rem', textAlign: 'center' }}>
                    <div><div style={{ fontSize: '0.65rem', color: 'var(--text-muted)', marginBottom: '4px' }}>MEAN</div><div style={{ fontWeight: 600 }}>{formatNumber(s.mean)}</div></div>
                    <div><div style={{ fontSize: '0.65rem', color: 'var(--text-muted)', marginBottom: '4px' }}>MEDIAN</div><div style={{ fontWeight: 600 }}>{s.median ?? 'n/a'}</div></div>
                    <div><div style={{ fontSize: '0.65rem', color: 'var(--text-muted)', marginBottom: '4px' }}>NULLS</div><div style={{ fontWeight: 600, color: s.null_pct > 0 ? 'var(--danger)' : 'var(--success)' }}>{s.null_pct ?? 0}%</div></div>
                    <div><div style={{ fontSize: '0.65rem', color: 'var(--text-muted)', marginBottom: '4px' }}>RANGE</div><div style={{ fontWeight: 600, fontSize: '0.8rem' }}>{s.min}–{s.max}</div></div>
                  </div>
                ) : (
                  <div style={{ fontSize: '0.85rem' }}>
                    <div style={{ color: 'var(--text-muted)', marginBottom: '8px', fontSize: '0.65rem' }}>TOP FREQUENCY DISTRIBUTION</div>
                    <div style={{ display: 'flex', flexWrap: 'wrap', gap: '8px' }}>
                        {Object.entries(s.top_values || {}).map(([v, count]) => (
                          <span key={v} className="badge badge-green" style={{ textTransform: 'none', background: 'rgba(16,185,129,0.05)', border: '1px solid rgba(16,185,129,0.2)' }}>{v}: {count}</span>
                        ))}
                    </div>
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      </div>

      <div className="card">
        <h3 style={{ marginBottom: '1.5rem', display: 'flex', alignItems: 'center', gap: '8px', color: 'white' }}><Database size={18} /> Raw Data Explorer (Top 100 Rows)</h3>
        <div className="data-preview-table-container">
          {data && data.length > 0 ? (
            <table className="data-preview-table">
              <thead>
                <tr>
                  {Object.keys(data[0]).map(k => (
                    <th key={k}>{k}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {data.map((row, i) => (
                  <tr key={i}>
                    {Object.values(row).map((v, j) => (
                      <td key={j}>
                        {typeof v === 'boolean' ? (v ? '✅ True' : '❌ False') : (v === null ? <em style={{ opacity: 0.5 }}>null</em> : String(v))}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          ) : <div style={{ padding: '2rem', textAlign: 'center', color: 'var(--text-muted)' }}>No records found in this dataset blob.</div>}
        </div>
      </div>
    </div>
  );
}

function ProjectsTab({ token, onUnauthorized }) {
  const [projects, setProjects] = useState([]);
  const [selectedProject, setSelectedProject] = useState(null);
  const [commits, setCommits] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  useEffect(() => {
    let cancelled = false;

    async function loadProjects() {
      setError('');

      try {
        const data = await fetchJson(`${API}/projects/`, {
          headers: createAuthHeaders(token),
        });
        if (cancelled) return;
        setProjects(Array.isArray(data) ? data : []);
      } catch (err) {
        console.error(err);
        if (cancelled) return;
        if (handleUnauthorizedError(err, onUnauthorized)) return;
        setProjects([]);
        setError(err.message || 'Unable to load projects.');
      }
    }

    loadProjects();

    return () => {
      cancelled = true;
    };
  }, [token, onUnauthorized]);

  const loadLog = async (name) => {
    setLoading(true);
    setSelectedProject(name);
    try {
      const data = await fetchJson(`${API}/projects/${name}/log`, {
        headers: createAuthHeaders(token),
      });
      setCommits(Array.isArray(data) ? data : []);
    } catch (e) {
      if (handleUnauthorizedError(e, onUnauthorized)) return;
      alert(e.message);
    }
    finally { setLoading(false); }
  };

  if (selectedProject) {
    return (
      <div>
        <button onClick={() => setSelectedProject(null)} className="nav-item" style={{ marginBottom: '1.5rem', width: 'auto', display: 'flex', gap: '8px', paddingLeft: 0, background: 'transparent' }}>
          <ArrowLeft size={18} /> Back to Projects
        </button>
        <h1>{selectedProject} History</h1>
        {loading ? <p>Loading commit graph...</p> : (
          <div className="commit-list">
            {commits.map(c => (
              <div key={c.commit_hash} className="card commit-card" style={{ borderLeft: '4px solid var(--accent-primary)', marginBottom: '1rem' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '0.5rem' }}>
                  <code style={{ background: 'rgba(0,0,0,0.3)', padding: '4px 8px', borderRadius: '4px', color: 'var(--accent-primary)' }}>{c.commit_hash.slice(0, 10)}</code>
                  <span className="muted">{new Date(c.created_at).toLocaleString()}</span>
                </div>
                <h3 style={{ margin: '0.5rem 0' }}>{c.message}</h3>
                <div style={{ fontSize: '0.85rem', color: 'var(--text-secondary)' }}>
                  Author: <span style={{ color: 'white' }}>{c.author}</span> · Branch: <span className="badge badge-blue">{c.branch}</span>
                </div>
                {(c.metadata || []).map((m, idx) => (
                  <div key={idx} style={{ marginTop: '1rem', padding: '0.75rem', background: 'rgba(255,255,255,0.03)', borderRadius: '8px', fontSize: '0.85rem' }}>
                    <strong>{m.file_name}</strong>: {formatNumber(m.row_count)} rows, {formatNumber(m.column_count)} columns.
                    {m.ai_summary && <p style={{ marginTop: '0.5rem', fontStyle: 'italic', opacity: 0.8 }}>{m.ai_summary.slice(0, 100)}...</p>}
                  </div>
                ))}
              </div>
            ))}
          </div>
        )}
      </div>
    );
  }

  return (
    <div>
      <h1>Projects & Commits</h1>
      <p style={{ marginBottom: '2rem' }}>Immutable history of your repositories.</p>
      {error && <p style={{ color: 'var(--danger)', marginBottom: '1rem' }}>{error}</p>}
      <div className="stats-grid">
        {projects.length === 0 && <p className="muted">No projects found.</p>}
        {projects.map(p => (
          <div key={p.id} className="card project-card" onClick={() => loadLog(p.name)} style={{ cursor: 'pointer' }}>
            <h3 style={{ color: 'white' }}>{p.name}</h3>
            <p style={{ fontSize: '0.85rem', marginTop: '8px' }}>{p.description || 'No description available.'}</p>
            <div style={{ marginTop: '1rem', fontSize: '0.8rem', color: 'var(--accent-primary)' }}>Owner: {p.owner}</div>
            <div style={{ marginTop: '1rem', textAlign: 'right' }}>
              <button className="nav-item" style={{ display: 'inline-flex', gap: '4px', padding: '4px 8px', margin: 0, height: 'auto' }}>View Log <ChevronRight size={14} /></button>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

function DbaConsoleTab({ token, onUnauthorized }) {
  const [logs, setLogs] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  useEffect(() => {
    let cancelled = false;

    async function loadLogs() {
      setLoading(true);
      setError('');

      try {
        const data = await fetchJson(`${API}/auth/audit`, {
          headers: createAuthHeaders(token),
        });
        if (cancelled) return;
        setLogs(Array.isArray(data) ? data : []);
      } catch (err) {
        console.error(err);
        if (cancelled) return;
        if (handleUnauthorizedError(err, onUnauthorized)) return;
        setLogs([]);
        setError(err.message || 'Unable to load audit logs.');
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    loadLogs();

    return () => {
      cancelled = true;
    };
  }, [token, onUnauthorized]);

  return (
    <div>
      <h1>DBA Audit Logs</h1>
      <p style={{ marginBottom: '2rem' }}>System-wide immutable ledger of all metadata modifications.</p>
      {error && <p style={{ color: 'var(--danger)', marginBottom: '1rem' }}>{error}</p>}
      
      <div className="card" style={{ padding: 0, overflow: 'hidden' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          <thead style={{ background: 'rgba(255,255,255,0.05)' }}>
            <tr>
              <th className="th-cell">Performed At</th>
              <th className="th-cell">User</th>
              <th className="th-cell">Action</th>
              <th className="th-cell">Table</th>
              <th className="th-cell">Details</th>
            </tr>
          </thead>
          <tbody>
            {logs.map(l => (
              <tr key={l.id} className="tr-row">
                <td className="td-cell muted">{new Date(l.performed_at).toLocaleString()}</td>
                <td className="td-cell"><strong>{l.user}</strong></td>
                <td className="td-cell"><span className={`badge ${l.action === 'COMMIT' ? 'badge-green' : 'badge-blue'}`}>{l.action}</span></td>
                <td className="td-cell muted">{l.table_name}</td>
                <td className="td-cell small">{JSON.stringify(l.details)}</td>
              </tr>
            ))}
            {loading && <tr><td colSpan="5" className="td-cell" style={{ textAlign: 'center' }}>Syncing with database logs...</td></tr>}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function AiChatTab({ token, onUnauthorized }) {
  const [msg, setMsg] = useState('');
  const [chat, setChat] = useState([]);
  const [loading, setLoading] = useState(false);
  const [datasets, setDatasets] = useState([]);
  const [selectedDatasetId, setSelectedDatasetId] = useState('');
  const chatEndRef = React.useRef(null);
  const scrollRef = React.useRef(null);

  const scrollToLastMessage = () => {
    if (chat.length > 0) {
      scrollRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }
  };

  useEffect(() => {
    if (!loading && chat.length > 0 && chat[chat.length - 1].role === 'bot') {
        scrollToLastMessage();
    }
  }, [chat, loading]);

  useEffect(() => {
    let cancelled = false;

    async function loadDatasets() {
      try {
        const data = await fetchJson(`${API}/metadata/`, {
          headers: createAuthHeaders(token),
        });
        if (cancelled) return;
        setDatasets(Array.isArray(data) ? data : []);
      } catch (err) {
        console.error(err);
        if (cancelled) return;
        if (handleUnauthorizedError(err, onUnauthorized)) return;
        setDatasets([]);
      }
    }

    loadDatasets();

    return () => {
      cancelled = true;
    };
  }, [token, onUnauthorized]);

  const send = async (e) => {
    e.preventDefault();
    if (!msg.trim()) return;
    const q = msg;
    setMsg('');
    setChat(c => [...c, { role: 'user', text: q }]);
    setLoading(true);
    
    try {
      const data = await fetchJson(`${API}/ai/chat`, {
        method: 'POST', headers: createAuthHeaders(token, { 'Content-Type': 'application/json' }),
        body: JSON.stringify({ 
          question: q, 
          metadata_id: selectedDatasetId ? parseInt(selectedDatasetId) : null 
        })
      });
      setChat(c => [...c, { role: 'bot', text: data?.answer || 'No response received.' }]);
    } catch (err) {
      if (handleUnauthorizedError(err, onUnauthorized)) return;
      setChat(c => [...c, { role: 'bot', text: 'Failed to connect to AI.' }]);
    } finally { setLoading(false); }
  };

  return (
    <div style={{ height: 'calc(100vh - 160px)', display: 'flex', flexDirection: 'column' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1.5rem' }}>
        <div>
          <h1>AI Data Agent</h1>
          <p>Ask questions about any of your committed datasets.</p>
        </div>
        <div className="form-group" style={{ margin: 0, minWidth: '300px' }}>
          <select 
            className="form-input" 
            value={selectedDatasetId} 
            onChange={e => setSelectedDatasetId(e.target.value)}
            style={{ height: '45px' }}
          >
            <option value="">Select a dataset for context (Optional)</option>
            {datasets.map(d => (
              <option key={d.id} value={d.id}>{d.file_name} (ID: {d.id})</option>
            ))}
          </select>
        </div>
      </div>
      
      <div className="card" style={{ flex: 1, display: 'flex', flexDirection: 'column', padding: '1rem', overflow: 'hidden', background: 'rgba(0,0,0,0.2)' }}>
        <div style={{ flex: 1, overflowY: 'auto', padding: '1rem', display: 'flex', flexDirection: 'column', gap: '1.5rem' }}>
          <div style={{ alignSelf: 'flex-start', background: 'rgba(99,102,241,0.1)', border: '1px solid rgba(99,102,241,0.2)', padding: '1.25rem', borderRadius: '12px', maxWidth: '80%', fontSize: '0.95rem' }}>
            Hi! I'm your DataHub AI Agent. Ask me anything about the datasets you've indexed!
          </div>
          {chat.map((m, i) => (
            <div 
              key={i} 
              ref={i === chat.length - 1 ? scrollRef : null}
              style={{ 
                alignSelf: m.role === 'user' ? 'flex-end' : 'flex-start',
                background: m.role === 'user' ? 'var(--accent-primary)' : 'rgba(255,255,255,0.05)',
                color: 'white', padding: '1.25rem', borderRadius: '12px', maxWidth: m.role === 'user' ? '80%' : '95%', wordBreak: 'break-word',
                border: m.role === 'user' ? 'none' : '1px solid var(--border-light)',
                boxShadow: m.role === 'user' ? '0 4px 12px rgba(99, 102, 241, 0.2)' : 'none'
              }}
            >
              {m.role === 'user' ? m.text : (
                <div className="markdown-container">
                  <ReactMarkdown remarkPlugins={[remarkGfm]}>
                    {m.text}
                  </ReactMarkdown>
                </div>
              )}
            </div>
          ))}
          {loading && (
            <div style={{ alignSelf: 'flex-start', display: 'flex', alignItems: 'center', gap: '8px', color: 'var(--text-muted)', background: 'rgba(255,255,255,0.03)', padding: '8px 16px', borderRadius: '20px' }}>
              <Loader2 size={14} className="spinner" /> Agent is analyzing...
            </div>
          )}
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
function AuthPage({ onAuth, apiStatus, notice }) {
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

        {notice && <div style={{ color: '#fde68a', background: 'rgba(245, 158, 11, 0.1)', padding: '12px', borderRadius: '8px', marginBottom: '1rem', fontSize: '0.9rem', border: '1px solid rgba(245, 158, 11, 0.2)' }}>{notice}</div>}
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
            {isLogin ? 'Sign In Securely' : 'Create Account'}
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
