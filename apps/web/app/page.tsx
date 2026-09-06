export default function HomePage() {
  const services = [
    {
      name: 'API Service',
      role: 'HTTP routing, payload validation, health checks',
      boundary: 'Does NOT execute CI jobs or interact directly with workers',
      status: 'PR 01 Shell',
    },
    {
      name: 'Scheduler',
      role: 'Process lifecycle, future queue & worker coordination',
      boundary: 'Does NOT own HTTP concerns or direct DB writes',
      status: 'PR 01 Shell',
    },
    {
      name: 'Worker',
      role: 'Process lifecycle, future executor container orchestration',
      boundary: 'Does NOT contain API routing or scheduler decisions',
      status: 'PR 01 Shell',
    },
    {
      name: 'CLI',
      role: 'Developer entrypoint, argument parsing, help/version',
      boundary: 'Workflow commands (run, logs, deploy) planned for future PRs',
      status: 'PR 01 Shell',
    },
    {
      name: 'Web UI',
      role: 'Next.js frontend shell for monitoring & visualization',
      boundary: 'Does NOT access persistence layer directly',
      status: 'PR 01 Shell',
    },
  ];

  return (
    <main className="container">
      <header className="hero">
        <div className="badge">
          <span className="badge-dot" />
          <span>PR 01: Repository Foundation & Architecture Contract</span>
        </div>
        <h1 className="title">Forge V2 Orchestration Engine</h1>
        <p className="subtitle">
          A distributed, self-hosted CI/CD engine built on strict architectural boundaries,
          reproducible workspaces, and typed contracts.
        </p>
      </header>

      <section className="grid">
        {services.map((svc) => (
          <article key={svc.name} className="card">
            <div className="card-header">
              <h2 className="card-title">{svc.name}</h2>
              <span className="status-tag status-active">{svc.status}</span>
            </div>
            <p className="card-description">{svc.role}</p>
            <div className="card-boundary">
              <strong>Boundary:</strong> {svc.boundary}
            </div>
          </article>
        ))}
      </section>

      <footer className="footer">
        <p>Forge V2 &bull; Monorepo Foundation &bull; Node.js &bull; TypeScript</p>
      </footer>
    </main>
  );
}
