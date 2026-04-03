# Swedish Financial Regulation MCP

**Swedish financial regulation data for AI compliance tools.**

[![npm version](https://badge.fury.io/js/%40ansvar%2Fswedish-financial-regulation-mcp.svg)](https://www.npmjs.com/package/@ansvar/swedish-financial-regulation-mcp)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![CI](https://github.com/Ansvar-Systems/swedish-financial-regulation-mcp/actions/workflows/ci.yml/badge.svg)](https://github.com/Ansvar-Systems/swedish-financial-regulation-mcp/actions/workflows/ci.yml)

Query Swedish financial regulation data -- regulations, decisions, and requirements from Finansinspektionen (Swedish Financial Supervisory Authority) -- directly from Claude, Cursor, or any MCP-compatible client.

Built by [Ansvar Systems](https://ansvar.eu) -- Stockholm, Sweden

---

## Quick Start

### Use Remotely (No Install Needed)

> Connect directly to the hosted version -- zero dependencies, nothing to install.

**Endpoint:** `https://mcp.ansvar.eu/swedish-financial-regulation/mcp`

| Client | How to Connect |
|--------|---------------|
| **Claude.ai** | Settings > Connectors > Add Integration > paste URL |
| **Claude Code** | `claude mcp add swedish-financial-regulation-mcp --transport http https://mcp.ansvar.eu/swedish-financial-regulation/mcp` |
| **Claude Desktop** | Add to config (see below) |
| **GitHub Copilot** | Add to VS Code settings (see below) |

**Claude Desktop** -- add to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "swedish-financial-regulation-mcp": {
      "type": "url",
      "url": "https://mcp.ansvar.eu/swedish-financial-regulation/mcp"
    }
  }
}
```

**GitHub Copilot** -- add to VS Code `settings.json`:

```json
{
  "github.copilot.chat.mcp.servers": {
    "swedish-financial-regulation-mcp": {
      "type": "http",
      "url": "https://mcp.ansvar.eu/swedish-financial-regulation/mcp"
    }
  }
}
```

### Use Locally (npm)

```bash
npx @ansvar/swedish-financial-regulation-mcp
```

**Claude Desktop** -- add to `claude_desktop_config.json`:

**macOS:** `~/Library/Application Support/Claude/claude_desktop_config.json`
**Windows:** `%APPDATA%\Claude\claude_desktop_config.json`

```json
{
  "mcpServers": {
    "swedish-financial-regulation-mcp": {
      "command": "npx",
      "args": ["-y", "@ansvar/swedish-financial-regulation-mcp"]
    }
  }
}
```

**Cursor / VS Code:**

```json
{
  "mcp.servers": {
    "swedish-financial-regulation-mcp": {
      "command": "npx",
      "args": ["-y", "@ansvar/swedish-financial-regulation-mcp"]
    }
  }
}
```

---

## Available Tools (6)

| Tool | Description |
|------|-------------|
| `se_fin_search_regulations` | Full-text search across Swedish financial regulation provisions. Returns matching Finansinspektionen FFFS foreskrifte... |
| `se_fin_get_regulation` | Get a specific Swedish financial regulation provision by sourcebook and reference. Accepts references like |
| `se_fin_list_sourcebooks` | List all Swedish financial regulation sourcebooks with their names and descriptions. Covers Finansinspektionen FFFS, ... |
| `se_fin_search_enforcement` | Search Finansinspektionen enforcement actions — ingripanden, sanktionsavgifter, and anmarkningar. Returns matching en... |
| `se_fin_check_currency` | Check whether a specific Swedish financial regulation provision reference is currently in force. Returns status and e... |
| `se_fin_about` | Return metadata about this MCP server: version, data source, tool list. |

All tools return structured data with source references and timestamps.

---

## Data Sources and Freshness

All content is sourced from official Swedish regulatory publications:

- **Finansinspektionen (Swedish Financial Supervisory Authority)** -- Official regulatory authority

### Data Currency

- Database updates are periodic and may lag official publications
- Freshness checks run via GitHub Actions workflows
- Last-updated timestamps in tool responses indicate data age

See `sources.yml` for full provenance metadata.

---

## Security

This project uses multiple layers of automated security scanning:

| Scanner | What It Does | Schedule |
|---------|-------------|----------|
| **CodeQL** | Static analysis for security vulnerabilities | Weekly + PRs |
| **Semgrep** | SAST scanning (OWASP top 10, secrets, TypeScript) | Every push |
| **Gitleaks** | Secret detection across git history | Every push |
| **Trivy** | CVE scanning on filesystem and npm dependencies | Daily |
| **Docker Security** | Container image scanning + SBOM generation | Daily |
| **Socket.dev** | Supply chain attack detection | PRs |
| **Dependabot** | Automated dependency updates | Weekly |

See [SECURITY.md](SECURITY.md) for the full policy and vulnerability reporting.

---

## Important Disclaimers

### Not Regulatory Advice

> **THIS TOOL IS NOT REGULATORY OR LEGAL ADVICE**
>
> Regulatory data is sourced from official publications by Finansinspektionen (Swedish Financial Supervisory Authority). However:
> - This is a **research tool**, not a substitute for professional regulatory counsel
> - **Verify all references** against primary sources before making compliance decisions
> - **Coverage may be incomplete** -- do not rely solely on this for regulatory research

**Before using professionally, read:** [DISCLAIMER.md](DISCLAIMER.md) | [PRIVACY.md](PRIVACY.md)

### Confidentiality

Queries go through the Claude API. For privileged or confidential matters, use on-premise deployment. See [PRIVACY.md](PRIVACY.md) for details.

---

## Development

### Setup

```bash
git clone https://github.com/Ansvar-Systems/swedish-financial-regulation-mcp
cd swedish-financial-regulation-mcp
npm install
npm run build
npm test
```

### Running Locally

```bash
npm run dev                                       # Start MCP server
npx @anthropic/mcp-inspector node dist/index.js   # Test with MCP Inspector
```

### Data Management

```bash
npm run build:db       # Rebuild SQLite database from seed data
npm run check-updates  # Check for new regulatory data
```

---

## Related Projects

This server is part of **Ansvar's MCP fleet** -- 276 MCP servers covering law, regulation, and compliance across 119 jurisdictions.

### Law MCPs

Full national legislation for 108 countries. Example: [@ansvar/swedish-law-mcp](https://github.com/Ansvar-Systems/swedish-law-mcp) -- 2,415 Swedish statutes with EU cross-references.

### Sector Regulator MCPs

National regulatory authority data for 29 EU/EFTA countries across financial regulation, data protection, cybersecurity, and competition. This MCP is one of 116 sector regulator servers.

### Domain MCPs

Specialized compliance domains: [EU Regulations](https://github.com/Ansvar-Systems/EU_compliance_MCP), [Security Frameworks](https://github.com/Ansvar-Systems/security-frameworks-mcp), [Automotive Cybersecurity](https://github.com/Ansvar-Systems/Automotive-MCP), [OT/ICS Security](https://github.com/Ansvar-Systems/ot-security-mcp), [Sanctions](https://github.com/Ansvar-Systems/Sanctions-MCP), and more.

Browse the full fleet at [mcp.ansvar.eu](https://mcp.ansvar.eu).

---

## Contributing

Contributions welcome! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

---

## License

Apache License 2.0. See [LICENSE](./LICENSE) for details.

### Data Licenses

Regulatory data sourced from official government publications. See `sources.yml` for per-source licensing details.

---

## About Ansvar Systems

We build AI-powered compliance and legal research tools for the European market. Our MCP fleet provides structured, verified regulatory data to AI assistants -- so compliance professionals can work with accurate sources instead of guessing.

**[ansvar.eu](https://ansvar.eu)** -- Stockholm, Sweden

---

<p align="center">
  <sub>Built with care in Stockholm, Sweden</sub>
</p>
